"""scrobble_sync_v2.py

Sync Navidrome playcounts with Last.fm in a single pass per track.

Flow:
  1. Fetch all Navidrome songs (merged by artist+title).
  2. For each Navidrome song, resolve its canonical Last.fm entry
     (track.getInfo, with fuzzy fallback) and record the mapping.
     - If Last.fm < Navidrome: scrobble the missing plays.
     - If Last.fm > Navidrome and --delete-excess is set: delete surplus
       scrobbles on the canonical entry.
  3. If --delete-orphans is set: fetch all Last.fm tracks and delete
     scrobbles for entries that do not map back to any Navidrome track.

The Last.fm web login (password prompt) is only performed when one of
the delete options is given.
"""
import argparse
import getpass
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import pylast
import requests
from pylast import WSError
from rapidfuzz import fuzz
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

import config

# ----------------------------
# Last.fm setup
# ----------------------------
_password_hash = (
    config.LASTFM_HASHED_PASSWORD
    if len(config.LASTFM_HASHED_PASSWORD) > 0
    else pylast.md5(config.LASTFM_PASSWORD)
)
network = pylast.LastFMNetwork(
    api_key=config.LASTFM_API_KEY,
    api_secret=config.LASTFM_API_SECRET,
    username=config.LASTFM_USERNAME,
    password_hash=_password_hash,
)

# ----------------------------
# Navidrome setup
# ----------------------------
NAVIDROME_PAGE_COUNT = 500
MAX_WORKERS = 5  # parallel album-song fetches
navidrome_session = requests.Session()


# ----------------------------
# Generic helpers
# ----------------------------
def retry_with_backoff(func, max_retries=6, initial_delay=1, backoff_factor=3, *args, **kwargs):
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (RequestException, ConnectionError, Timeout, WSError, httpx.RequestError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                print(f"  ⚠️  Network error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                print(f"  ⏳ Retrying in {delay}s...")
                time.sleep(delay)
                delay *= backoff_factor
            else:
                print(f"  ❌ Failed after {max_retries} attempts: {str(e)[:100]}")
        except HTTPError as e:
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                if status_code == 429:
                    retry_after = int(e.response.headers.get('Retry-After', delay * 2))
                    print(f"  ⚠️  Rate limited (429). Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    if attempt < max_retries - 1:
                        continue
                elif 500 <= status_code < 600:
                    if attempt < max_retries - 1:
                        print(f"  ⚠️  Server error {status_code} (attempt {attempt + 1}/{max_retries})")
                        print(f"  ⏳ Retrying in {delay}s...")
                        time.sleep(delay)
                        delay *= backoff_factor
                        continue
            last_exception = e
            print(f"  ❌ HTTP error: {str(e)[:100]}")
            break
        except Exception as e:
            last_exception = e
            print(f"  ❌ Unexpected error: {str(e)[:100]}")
            break

    raise last_exception if last_exception else Exception("Retry failed with no exception")


_normalize_cache = {}

def normalize_cached(s):
    if s not in _normalize_cache:
        _normalize_cache[s] = s.lower().strip()
    return _normalize_cache[s]


def fuzzy_match(a, b):
    return fuzz.token_sort_ratio(normalize_cached(a), normalize_cached(b))


def _encode_plus(s):
    """Replace + with %2B to survive Last.fm's broken form decoder.

    Last.fm's API decodes POST bodies in the wrong order: percent-decode
    first (%2B → +), THEN form-decode (+ → space).  This destroys any
    literal + in track/artist names.

    By pre-replacing + with %2B in parameter values, httpx form-encodes
    the % as %25, producing %252B in the wire body.  The server's decoder
    then does: %252B → %2B (percent-decode) → %2B (no + to replace).
    The value arrives as the literal string "%2B", which matches how
    Last.fm already stores these tracks.
    """
    return s.replace('+', '%2B')


def _normalize_aggressive(s):
    """Collapse encoding artifacts for matching: +, %2B, extra whitespace."""
    s = s.lower().strip()
    s = s.replace('%2b', ' ').replace('+', ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    return s


# ----------------------------
# Last.fm API helpers
# ----------------------------
def _call_track_getinfo(track):
    """Call track.getInfo and extract canonical artist, title, and user playcount.
    Returns (canonical_artist, canonical_title, userplaycount).
    Raises WSError if the track doesn't exist on Last.fm."""
    params = track._get_params()
    params["username"] = track.username

    # Work around Last.fm's broken form decoder (see _encode_plus)
    if '+' in params.get("track", "") or '+' in params.get("artist", ""):
        params["track"] = _encode_plus(params.get("track", ""))
        params["artist"] = _encode_plus(params.get("artist", ""))

    doc = track._request("track.getInfo", True, params)

    playcount = 0
    canonical_artist = str(track.artist)
    canonical_title = str(track.title)

    try:
        nodes = doc.getElementsByTagName("userplaycount")
        if nodes and nodes[0].firstChild:
            playcount = int(nodes[0].firstChild.data.strip())
    except (IndexError, ValueError):
        pass

    try:
        name_nodes = doc.getElementsByTagName("name")
        if name_nodes and name_nodes[0].firstChild:
            canonical_title = name_nodes[0].firstChild.data.strip()
    except IndexError:
        pass

    try:
        artist_nodes = doc.getElementsByTagName("artist")
        if artist_nodes:
            artist_name_nodes = artist_nodes[0].getElementsByTagName("name")
            if artist_name_nodes and artist_name_nodes[0].firstChild:
                canonical_artist = artist_name_nodes[0].firstChild.data.strip()
    except IndexError:
        pass

    return canonical_artist, canonical_title, playcount


def find_track(artist, title):
    """Resolve a Navidrome artist/title to its canonical Last.fm entry.

    Returns a pylast Track with ._canonical_artist / ._canonical_title /
    ._userplaycount set, or None if the track cannot be found.
    """
    def _find():
        try:
            track = network.get_track(artist, title)
            ca, ct, pc = _call_track_getinfo(track)
            track._canonical_artist = ca
            track._canonical_title = ct
            track._userplaycount = pc
            return track
        except WSError:
            results = network.search_for_track(
                artist_name=artist,
                track_name=title,
            ).get_next_page()

            for track in results[:10]:
                artist_val = fuzzy_match(artist, str(track.artist))
                title_val = fuzzy_match(title, str(track.title))

                if artist_val >= config.FUZZY_THRESHOLD and title_val >= config.FUZZY_THRESHOLD:
                    try:
                        ca, ct, pc = _call_track_getinfo(track)
                        track._canonical_artist = ca
                        track._canonical_title = ct
                        track._userplaycount = pc
                        print(f"  → matched! | {track.artist} | {track.title}")
                        return track
                    except WSError:
                        continue

        return None

    try:
        return retry_with_backoff(_find, max_retries=3)
    except Exception:
        print(f"  ❌ Failed to find track after retries: {artist} - {title}")
        return None


def fetch_lastfm_tracks():
    """Fetch all unique tracks from Last.fm with playcounts.

    Uses user.get_top_tracks(period=OVERALL, limit=None) which paginates
    through the entire library.  Returns [{artist, title, playcount}].
    """
    print("Fetching all tracks from Last.fm...")
    user = network.get_user(config.LASTFM_USERNAME)

    def _fetch():
        return user.get_top_tracks(period=pylast.PERIOD_OVERALL, limit=None)

    top_items = retry_with_backoff(_fetch, max_retries=5, initial_delay=2)

    tracks = []
    for item in top_items:
        tracks.append({
            "artist": str(item.item.artist),
            "title": str(item.item.title),
            "playcount": int(item.weight),
        })
    print(f"  → {len(tracks)} unique tracks from Last.fm")
    return tracks


def _get_verified_scrobbles(user, artist, track, prefix=""):
    """Get scrobbles that actually belong to the queried artist/track entry.

    Last.fm's API auto-corrects track names silently; this filters out
    results whose returned name doesn't match the one we queried.
    """
    scrobbles = retry_with_backoff(
        lambda: user.get_track_scrobbles(artist=artist, track=track),
        max_retries=5,
        initial_delay=2,
    )

    verified = []
    auto_corrected = False
    queried_artist = artist.lower().strip()
    queried_title = track.lower().strip()

    for s in scrobbles:
        returned_artist = str(s.track.artist).lower().strip()
        returned_title = str(s.track.title).lower().strip()
        if returned_artist != queried_artist or returned_title != queried_title:
            if not auto_corrected:
                print(f"{prefix} | ⚠️  API auto-corrected: queried '{artist} - {track}' → got '{s.track.artist} - {s.track.title}' — skipping these")
                auto_corrected = True
            continue
        verified.append(s)

    return verified


# ----------------------------
# Navidrome API
# ----------------------------
def api_call(endpoint, params):
    base = {
        "u": config.NAVIDROME_USERNAME,
        "p": config.NAVIDROME_PASSWORD,
        "v": "1.16.1",
        "c": "ND2LastFM",
        "f": "json",
    }

    def _make_request():
        try:
            r = navidrome_session.get(
                f"{config.NAVIDROME_API_URL}/{endpoint}",
                params={**base, **params},
                timeout=30,
            )
            r.raise_for_status()

            json_response = r.json()
            if "subsonic-response" not in json_response:
                raise ValueError("Invalid API response: missing 'subsonic-response'")

            response_data = json_response["subsonic-response"]
            if response_data.get("status") == "failed":
                error = response_data.get("error", {})
                raise Exception(f"Navidrome API error: {error.get('message', 'Unknown error')}")

            return response_data
        except requests.exceptions.JSONDecodeError as e:
            raise Exception(f"Invalid JSON response from Navidrome: {str(e)}")

    return retry_with_backoff(_make_request, max_retries=3)


def fetch_album_songs(album_id):
    try:
        album_resp = api_call("getAlbum.view", {"id": album_id})
        songs = album_resp.get("album", {}).get("song", [])

        tracks = []
        for s in songs:
            playcount = s.get("playCount", 0)
            if playcount > 0:
                tracks.append({
                    "artist": s.get("artist"),
                    "title": s.get("title"),
                    "playcount": playcount,
                })
        return tracks
    except Exception as e:
        print(f"  ⚠️  Failed to fetch album {album_id}: {str(e)[:100]}")
        return []


def fetch_all_tracks():
    """Fetch all Navidrome albums, then songs per album; merge by artist+title.
    Returns [{artist, title, playcount}]."""
    tracks = []
    offset = 0
    size = NAVIDROME_PAGE_COUNT

    while True:
        try:
            resp = api_call(
                "getAlbumList2.view",
                {"type": "alphabeticalByName", "offset": offset, "size": size},
            )

            albums = resp.get("albumList2", {}).get("album", [])
            if not albums:
                break

            print(f"  Fetching songs for {len(albums)} albums in parallel...")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_album = {
                    executor.submit(fetch_album_songs, album["id"]): album["id"]
                    for album in albums
                }
                for future in as_completed(future_to_album):
                    tracks.extend(future.result())

            print(f"  → Fetched {len(tracks)} total tracks so far")

            offset += len(albums)
            if len(albums) < size:
                break
        except Exception as e:
            print(f"  ❌ Failed to fetch album list at offset {offset}: {str(e)[:100]}")
            if len(tracks) > 0:
                print(f"  → Proceeding with {len(tracks)} tracks already fetched")
                break
            else:
                raise

    # Merge duplicates (Last.fm doesn't distinguish by album)
    merged = {}
    for t in tracks:
        key = (t["artist"].lower().strip(), t["title"].lower().strip())
        if key not in merged:
            merged[key] = {"artist": t["artist"], "title": t["title"], "playcount": t["playcount"]}
        else:
            merged[key]["playcount"] += t["playcount"]
            if t["playcount"] > merged[key]["playcount"] - t["playcount"]:
                merged[key]["artist"] = t["artist"]
                merged[key]["title"] = t["title"]

    tracks = list(merged.values())
    print(f"  → {len(tracks)} unique tracks after merging duplicates")
    return tracks


# ----------------------------
# Last.fm web session (for deletions)
# ----------------------------
def get_lastfm_web_session(password):
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    login_page = session.get("https://www.last.fm/login")

    form_csrf = re.findall(
        r"name=[\"']csrfmiddlewaretoken[\"'][^>]*value=[\"']([^\"']+)",
        login_page.text,
    )
    if not form_csrf:
        raise Exception("Failed to get CSRF token from Last.fm login form")
    csrf_token = form_csrf[0]

    resp = session.post(
        "https://www.last.fm/login",
        data={
            "csrfmiddlewaretoken": csrf_token,
            "username_or_email": config.LASTFM_USERNAME,
            "password": password,
            "next": "/user/_",
        },
        headers={
            "Referer": "https://www.last.fm/login",
            "Origin": "https://www.last.fm",
        },
        allow_redirects=False,
    )

    if resp.status_code != 302:
        raise Exception("Last.fm web login failed - incorrect password")

    return session


_web_session_lock = threading.Lock()
_csrf_token = None


def _refresh_csrf_token(web_session):
    """Fetch a fresh CSRF token from the Last.fm library page."""
    global _csrf_token
    lib_page = web_session.get(f"https://www.last.fm/user/{config.LASTFM_USERNAME}/library")
    form_csrf = re.findall(
        r"name=[\"']csrfmiddlewaretoken[\"'][^>]*value=[\"']([^\"']+)",
        lib_page.text,
    )
    _csrf_token = form_csrf[0] if form_csrf else web_session.cookies.get("csrftoken")


def delete_scrobble(web_session, artist, title, timestamp, dry_run=False):
    global _csrf_token
    dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))

    if dry_run:
        print(f"  [DRY] would delete @ {dt}: {artist} - {title}")
        return True

    with _web_session_lock:
        if _csrf_token is None:
            _refresh_csrf_token(web_session)

        for attempt in range(3):
            resp = web_session.post(
                f"https://www.last.fm/user/{config.LASTFM_USERNAME}/library/delete",
                data={
                    "csrfmiddlewaretoken": _csrf_token,
                    "artist_name": artist,
                    "track_name": title,
                    "timestamp": str(timestamp),
                },
                headers={
                    "Referer": f"https://www.last.fm/user/{config.LASTFM_USERNAME}/library",
                    "Origin": "https://www.last.fm",
                    "X-Requested-With": "XMLHttpRequest",
                },
            )

            if resp.status_code == 200:
                print(f"  Deleted @ {dt}: {artist} - {title}")
                return True
            elif resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 10 * (attempt + 1)))
                print(f"  ⚠️  Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            elif resp.status_code == 403:
                print(f"  ⚠️  CSRF expired, refreshing token...")
                _refresh_csrf_token(web_session)
                continue
            else:
                print(f"  Failed to delete @ {dt}: {artist} - {title} (status {resp.status_code}: {resp.text[:200]})")
                return False

    print(f"  ❌ Failed to delete after retries @ {dt}: {artist} - {title}")
    return False


# ----------------------------
# Sync logic
# ----------------------------
def scrobble_track(canonical_artist, canonical_title, dry_run):
    """Send a single scrobble for the canonical Last.fm entry."""
    scrobble_artist = _encode_plus(canonical_artist)
    scrobble_title = _encode_plus(canonical_title)

    timestamp = int(time.time()) - random.randint(60, 60 * 60 * 24)
    dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))

    if dry_run:
        print(f"  [DRY] scrobble @ {dt}: {scrobble_artist} – {scrobble_title}")
        return

    def _scrobble():
        network.scrobble(
            artist=scrobble_artist,
            title=scrobble_title,
            timestamp=timestamp,
        )
        time.sleep(config.SCROBBLE_DELAY)

    retry_with_backoff(_scrobble, max_retries=5, initial_delay=2)
    print(f"  ✓ Scrobbled @ {dt}: {scrobble_artist} – {scrobble_title}")


def sync_track(nd, lf_by_agg, used_lf_keys, web_session, args, user, counters):
    """Process a single Navidrome track: resolve canonical + any wrong-version
    Last.fm entries sharing an aggressive key, then scrobble/delete as needed.
    All related Last.fm entries are added to used_lf_keys so orphan detection
    skips them.
    """
    nd_artist = nd["artist"]
    nd_title = nd["title"]
    nd_count = nd["playcount"]
    prefix = counters["prefix"]

    # 1. Resolve canonical via Last.fm API
    track = find_track(nd_artist, nd_title)
    canonical_artist = track._canonical_artist if track else None
    canonical_title = track._canonical_title if track else None
    canonical_count = track._userplaycount if track else 0

    # 2. Gather all Last.fm entries that share an aggressive key with either
    # the Navidrome track or the resolved canonical.  Skip entries already
    # claimed by an earlier Navidrome track.
    agg_keys = {(_normalize_aggressive(nd_artist), _normalize_aggressive(nd_title))}
    if canonical_artist:
        agg_keys.add((_normalize_aggressive(canonical_artist), _normalize_aggressive(canonical_title)))

    related_lf = []
    _seen = set()
    for ak in agg_keys:
        for lf in lf_by_agg.get(ak, []):
            lf_ek = (lf["artist"].lower().strip(), lf["title"].lower().strip())
            if lf_ek in used_lf_keys or lf_ek in _seen:
                continue
            _seen.add(lf_ek)
            related_lf.append(lf)

    # 3. If API lookup failed but Last.fm has a related entry, use the
    # highest-playcount one as the canonical.
    if track is None:
        if not related_lf:
            print(f"{prefix} | Not found on Last.fm, skipping")
            return
        best = max(related_lf, key=lambda x: x["playcount"])
        canonical_artist = best["artist"]
        canonical_title = best["title"]
        canonical_count = best["playcount"]
        print(f"{prefix} | API lookup failed, using top-tracks match '{canonical_artist} – {canonical_title}' ({canonical_count} plays)")

    canonical_ek = (canonical_artist.lower().strip(), canonical_title.lower().strip())
    wrong_lf = [lf for lf in related_lf
                if (lf["artist"].lower().strip(), lf["title"].lower().strip()) != canonical_ek]

    # 4. Mark everything as claimed so orphan phase skips them
    used_lf_keys.add(canonical_ek)
    for lf in wrong_lf:
        used_lf_keys.add((lf["artist"].lower().strip(), lf["title"].lower().strip()))

    wrong_count = sum(lf["playcount"] for lf in wrong_lf)
    lf_total = canonical_count + wrong_count

    # Effective Last.fm count for sync decisions: if we're going to delete
    # wrong versions, they're effectively gone — target canonical alone.
    will_delete_wrong = wrong_lf and args.delete_excess
    effective_lf = canonical_count if will_delete_wrong else lf_total

    # 5. Decide what to do with the canonical entry
    scrobble_delta = 0
    excess_to_delete = 0
    action = "OK"

    # Target is Navidrome count, but --enforce-cap lowers it to the config cap
    cap = config.MAX_SCORRBLES_PER_TRACK_TOTAL
    target = min(nd_count, cap) if args.enforce_cap else nd_count

    if effective_lf < target:
        if counters["max_reached"]:
            action = "skipping (max reached)"
        elif effective_lf >= cap:
            action = "skipping (≥ MAX_SCORRBLES_PER_TRACK_TOTAL)"
        else:
            delta = target - effective_lf
            delta = min(delta, config.MAX_SCROBBLES_PER_TRACK_PER_RUN)
            if args.max is not None:
                delta = min(delta, args.max - counters["scrobbled"])
            if delta > 0:
                scrobble_delta = delta
                action = f"+{delta}"
            else:
                action = "skipping (per-run/max limit)"
    elif effective_lf > target:
        excess_to_delete = effective_lf - target
        if args.delete_excess or args.enforce_cap:
            action = f"deleting {excess_to_delete} excess"
        else:
            action = f"+{excess_to_delete} excess (use --delete-excess to delete)"

    # 6. Print the status line
    if wrong_lf:
        wrong_desc = ", ".join(
            f"'{lf['artist']} – {lf['title']}' ({lf['playcount']})" for lf in wrong_lf
        )
        detail = f" (canonical={canonical_count}, wrong={wrong_count}: {wrong_desc})"
    else:
        detail = ""
    wrong_prefix = f"deleting {wrong_count} wrong + " if will_delete_wrong else ""
    print(f"{prefix} | Navidrome={nd_count} Last.fm={lf_total}{detail} → {wrong_prefix}{action}")

    # 7. Execute: delete wrong-version scrobbles
    if will_delete_wrong:
        for lf in wrong_lf:
            try:
                scrobbles = retry_with_backoff(
                    lambda lf=lf: user.get_track_scrobbles(artist=lf["artist"], track=lf["title"]),
                    max_retries=5,
                    initial_delay=2,
                )
            except Exception as e:
                print(f"  ⚠️  Failed to fetch wrong-version scrobbles for '{lf['artist']} – {lf['title']}': {str(e)[:100]}")
                continue
            for s in scrobbles:
                s_artist = str(s.track.artist)
                s_title = str(s.track.title)
                if delete_scrobble(web_session, s_artist, s_title, int(s.timestamp), args.dry_run):
                    counters["deleted"] += 1
                time.sleep(max(config.SCROBBLE_DELAY, 2))

    # 8. Execute: scrobble missing plays
    if scrobble_delta > 0:
        for _ in range(scrobble_delta):
            try:
                scrobble_track(canonical_artist, canonical_title, args.dry_run)
                counters["scrobbled"] += 1
                if args.max is not None and counters["scrobbled"] >= args.max:
                    counters["max_reached"] = True
                    print(f"  🛑 Reached scrobble limit ({args.max})")
                    break
            except Exception as e:
                print(f"  ⚠️  Scrobble failed: {str(e)[:100]}")
                break

    # 9. Execute: delete excess on the canonical entry
    if excess_to_delete > 0 and (args.delete_excess or args.enforce_cap):
        try:
            scrobbles = _get_verified_scrobbles(user, canonical_artist, canonical_title, prefix)
            timestamps = [int(s.timestamp) for s in scrobbles[:excess_to_delete]]
            if len(timestamps) < excess_to_delete:
                print(f"  ⚠️  Only found {len(timestamps)} timestamps, expected {excess_to_delete}")
            for ts in timestamps:
                if delete_scrobble(web_session, canonical_artist, canonical_title, ts, args.dry_run):
                    counters["deleted"] += 1
                time.sleep(max(config.SCROBBLE_DELAY, 2))
        except Exception as e:
            print(f"  ⚠️  Failed to delete excess: {str(e)[:100]}")


def run_orphan_phase(lf_tracks, used_lf_keys, web_session, args, user, counters):
    """Delete scrobbles for Last.fm entries that weren't claimed during sync."""
    print("\n=== Orphan detection ===")
    orphans = [
        lf for lf in lf_tracks
        if (lf["artist"].lower().strip(), lf["title"].lower().strip()) not in used_lf_keys
    ]

    print(f"Found {len(orphans)} orphan tracks (not in Navidrome)\n")

    for i, lf in enumerate(orphans, 1):
        prefix = f"[orphan {i}/{len(orphans)}]"
        print(f"{prefix} {lf['artist']} – {lf['title']} ({lf['playcount']} scrobbles)")
        try:
            scrobbles = retry_with_backoff(
                lambda lf=lf: user.get_track_scrobbles(artist=lf["artist"], track=lf["title"]),
                max_retries=5,
                initial_delay=2,
            )
        except Exception as e:
            print(f"  ⚠️  Failed to fetch scrobbles: {str(e)[:100]}")
            continue

        for s in scrobbles:
            s_artist = str(s.track.artist)
            s_title = str(s.track.title)
            if delete_scrobble(web_session, s_artist, s_title, int(s.timestamp), args.dry_run):
                counters["deleted"] += 1
            time.sleep(max(config.SCROBBLE_DELAY, 2))


def main(args):
    start_time = time.time()

    needs_login = args.delete_excess or args.delete_orphans or args.enforce_cap
    web_session = None
    if needs_login:
        password = getpass.getpass("Enter your Last.fm password: ")
        try:
            web_session = get_lastfm_web_session(password)
            print("Logged into Last.fm website\n")
        except Exception as e:
            print(f"❌ {e}")
            return

    if args.dry_run:
        print("🧪 DRY-RUN MODE: no scrobbles will be sent and no scrobbles will be deleted\n")

    # Phase 1: fetch Navidrome
    print("Fetching Navidrome tracks...")
    try:
        nd_tracks = fetch_all_tracks()
    except Exception as e:
        print(f"❌ Failed to fetch Navidrome tracks: {str(e)[:200]}")
        return
    print(f"  → {len(nd_tracks)} unique tracks\n")

    # Phase 2: fetch all Last.fm tracks and build an aggressive-key index.
    # We always fetch upfront so that wrong-version entries are visible to the
    # per-track sync (accurate Last.fm totals, not just canonical playcount).
    try:
        lf_tracks = fetch_lastfm_tracks()
    except Exception as e:
        print(f"❌ Failed to fetch Last.fm tracks: {str(e)[:200]}")
        return

    lf_by_agg = {}
    for lf in lf_tracks:
        key = (_normalize_aggressive(lf["artist"]), _normalize_aggressive(lf["title"]))
        lf_by_agg.setdefault(key, []).append(lf)
    print()

    # Phase 3: per-track sync
    used_lf_keys = set()
    user = network.get_user(config.LASTFM_USERNAME)
    counters = {"scrobbled": 0, "deleted": 0, "max_reached": False, "prefix": ""}

    total = len(nd_tracks)
    for i, nd in enumerate(nd_tracks, 1):
        counters["prefix"] = f"[{i}/{total}] {nd['artist']} – {nd['title']}"
        try:
            sync_track(nd, lf_by_agg, used_lf_keys, web_session, args, user, counters)
        except Exception as e:
            print(f"{counters['prefix']} | ❌ Error: {str(e)[:100]}")

    # Phase 4: orphans
    if args.delete_orphans:
        run_orphan_phase(lf_tracks, used_lf_keys, web_session, args, user, counters)

    duration = time.time() - start_time
    print(f"\n✅ Done. Scrobbled: {counters['scrobbled']}, Deleted: {counters['deleted']}")
    print(f"   Total execution time: {duration:.2f}s ({duration / 60:.2f} min)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Navidrome playcounts with Last.fm (v2)")
    parser.add_argument("--dry-run", action="store_true", help="Do not scrobble or delete, only print actions")
    parser.add_argument("--max", type=int, default=None, help="Stop scrobbling after N scrobbles (does not limit deletes)")
    parser.add_argument("--delete-excess", action="store_true", help="Delete excess scrobbles on canonical Last.fm entries")
    parser.add_argument("--delete-orphans", action="store_true", help="Delete scrobbles for Last.fm tracks that don't exist in Navidrome")
    parser.add_argument("--enforce-cap", action="store_true", help=f"Also delete down to MAX_SCORRBLES_PER_TRACK_TOTAL ({config.MAX_SCORRBLES_PER_TRACK_TOTAL}) when Last.fm exceeds it")

    main(parser.parse_args())
