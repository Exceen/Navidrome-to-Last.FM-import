import requests
from requests.exceptions import RequestException, Timeout, ConnectionError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import argparse
import pylast
from pylast import WSError

import config
from rapidfuzz import fuzz

# ----------------------------
# General settings
# ----------------------------
TEST_MODE = True

# ----------------------------
# Last.fm setup
# ----------------------------
password = config.LASTFM_HASHED_PASSWORD if len(config.LASTFM_HASHED_PASSWORD) > 0 else pylast.md5(config.LASTFM_PASSWORD)
network = pylast.LastFMNetwork(
    api_key=config.LASTFM_API_KEY,
    api_secret=config.LASTFM_API_SECRET,
    username=config.LASTFM_USERNAME,
    password_hash=password,
)
# ----------------------------
# Navidrome setup
# ----------------------------

NAVIDROME_PAGE_COUNT = 500
BREAK_AT_FIRST_PAGE = False
MAX_WORKERS = 5  # Number of parallel API calls for fetching albums
if TEST_MODE:
    NAVIDROME_PAGE_COUNT = 10
    BREAK_AT_FIRST_PAGE = True
    MAX_WORKERS = 3

# Persistent HTTP session for connection pooling
navidrome_session = requests.Session()

# ----------------------------
# Helpers
# ----------------------------

def retry_with_backoff(func, max_retries=3, initial_delay=1, backoff_factor=2, *args, **kwargs):
    """
    Retry a function with exponential backoff.
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (RequestException, ConnectionError, Timeout, WSError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                print(f"  ⚠️  Network error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                print(f"  ⏳ Retrying in {delay}s...")
                time.sleep(delay)
                delay *= backoff_factor
            else:
                print(f"  ❌ Failed after {max_retries} attempts: {str(e)[:100]}")
        except HTTPError as e:
            # Check if it's a rate limit error (429) or server error (5xx)
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                if status_code == 429:
                    # Rate limit - wait longer
                    retry_after = int(e.response.headers.get('Retry-After', delay * 2))
                    print(f"  ⚠️  Rate limited (429). Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    if attempt < max_retries - 1:
                        continue
                elif 500 <= status_code < 600:
                    # Server error - retry
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
            # Unexpected error - don't retry
            last_exception = e
            print(f"  ❌ Unexpected error: {str(e)[:100]}")
            break

    raise last_exception if last_exception else Exception("Retry failed with no exception")

def normalize(s):
    return s.lower().strip()

# Cache for normalized strings to avoid redundant normalization
_normalize_cache = {}

def normalize_cached(s):
    if s not in _normalize_cache:
        _normalize_cache[s] = s.lower().strip()
    return _normalize_cache[s]

def fuzzy_match(a, b):
    return fuzz.token_sort_ratio(normalize_cached(a), normalize_cached(b))

def find_track(artist, title):
    def _find():
        try:
            track = network.get_track(artist, title)
            track.get_userplaycount()  # verify it exists
            return track
        except WSError as wse:
            # print('track not found by exact match:', artist, '|', title)

            results = network.search_for_track(
                artist_name=artist,
                track_name=title,
            ).get_next_page()

            for track in results[:10]:

                # fuzzy_match(artist, artist) < config.FUZZY_THRESHOLD
                artist_val = fuzzy_match(artist, str(track.artist))
                title_val = fuzzy_match(title, str(track.title))

                # print(int(artist_val), int(title_val), track.artist, "-", track.title)

                if artist_val >= config.FUZZY_THRESHOLD and title_val >= config.FUZZY_THRESHOLD:
                    try:
                        track.get_userplaycount()  # verify it exists
                        print('  -> matched!', '|', track.artist, '|', track.title, '|')
                        return track
                    except WSError:
                        continue

        return None

    try:
        return retry_with_backoff(_find, max_retries=3)
    except Exception as e:
        print(f'  ❌ Failed to find track after retries: {artist} - {title}')
        return None


LASTFM_PLAYCOUNTS = {}
def get_lastfm_playcount(artist, title):
    if not((artist, title) in LASTFM_PLAYCOUNTS):
        try:
            track = find_track(artist, title)
            if track is None:
                LASTFM_PLAYCOUNTS[(artist, title)] = {}
            else:
                LASTFM_PLAYCOUNTS[(artist, title)] = {
                    'playcount': track.get_userplaycount() or 0,
                    'scrobbled_this_run_count': 0,
                }
        except Exception as e:
            print(f"  ⚠️  Error getting Last.fm playcount for {artist} - {title}: {str(e)[:100]}")
            LASTFM_PLAYCOUNTS[(artist, title)] = {}

    pc_data = LASTFM_PLAYCOUNTS[(artist, title)]
    if pc_data is None or 'playcount' not in pc_data:
        return None

    return pc_data['playcount'] + pc_data['scrobbled_this_run_count']


def scrobble_once(artist, title, dry_run):
    global LASTFM_PLAYCOUNTS

    # timestamp = int(time.time()) - random.randint(60, 60 * 60 * 2 * 1)  # random timestamp within the last 2 hours
    timestamp = int(time.time()) - random.randint(60, 60 * 60 * 24 * 1)  # random timestamp within last 1 day
    dt = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))

    if dry_run:
        print(f"  [DRY] scrobble @ {dt}: {artist} – {title} ")
    else:
        def _scrobble():
            network.scrobble(
                artist=artist,
                title=title,
                timestamp=timestamp,
            )
            time.sleep(config.SCROBBLE_DELAY)

        try:
            retry_with_backoff(_scrobble, max_retries=5, initial_delay=2)
            print(f"  ✓ Scrobbled @ {dt}: {artist} – {title}")
        except Exception as e:
            print(f"  ❌ Failed to scrobble after retries: {artist} – {title}")
            raise  # Re-raise to let the caller handle it

    LASTFM_PLAYCOUNTS[(artist, title)]['scrobbled_this_run_count'] += 1

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

            # Check for API-level errors
            response_data = json_response["subsonic-response"]
            if response_data.get("status") == "failed":
                error = response_data.get("error", {})
                raise Exception(f"Navidrome API error: {error.get('message', 'Unknown error')}")

            return response_data
        except requests.exceptions.JSONDecodeError as e:
            raise Exception(f"Invalid JSON response from Navidrome: {str(e)}")

    return retry_with_backoff(_make_request, max_retries=3)

def fetch_album_songs(album_id):
    """
    Fetch songs for a single album.
    Returns list of track dicts with playcount > 0.
    """
    try:
        album_resp = api_call("getAlbum.view", {"id": album_id})
        songs = album_resp.get("album", {}).get("song", [])

        tracks = []
        for s in songs:
            playcount = s.get("playCount", 0)
            if playcount > 0:
                tracks.append(
                    {
                        "artist": s.get("artist"),
                        "title": s.get("title"),
                        "playcount": playcount,
                    }
                )
        return tracks
    except Exception as e:
        print(f"  ⚠️  Failed to fetch album {album_id}: {str(e)[:100]}")
        return []

def fetch_all_tracks():
    """
    Fetches all albums, then all songs per album (in parallel).
    Returns [{artist, title, playcount}]
    """
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

            # Fetch songs for all albums in parallel
            print(f"  Fetching songs for {len(albums)} albums in parallel...")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all album fetches
                future_to_album = {
                    executor.submit(fetch_album_songs, album["id"]): album["id"]
                    for album in albums
                }

                # Collect results as they complete
                for future in as_completed(future_to_album):
                    album_tracks = future.result()
                    tracks.extend(album_tracks)

            print(f"  → Fetched {len(tracks)} total tracks so far")

            offset += len(albums)
            if len(albums) < size:
                break

            if BREAK_AT_FIRST_PAGE:
                break

        except Exception as e:
            print(f"  ❌ Failed to fetch album list at offset {offset}: {str(e)[:100]}")
            print(f"  → Will retry or skip if retries exhausted...")
            # If we already have some tracks, we can continue
            if len(tracks) > 0:
                print(f"  → Proceeding with {len(tracks)} tracks already fetched")
                break
            else:
                raise  # Re-raise if we have no tracks at all

    return tracks

# ----------------------------
# Main logic
# ----------------------------
def main(dry_run, max_scrobbles):
    start_time = time.time()

    if not dry_run:
        print("⚠️  LIVE MODE: scrobbles will be sent to Last.fm\n")
    else:
        print("🧪 DRY-RUN MODE: no scrobbles will be sent\n")

    try:
        fetch_start = time.time()
        tracks = fetch_all_tracks()
        fetch_duration = time.time() - fetch_start
        print(f"Fetched {len(tracks)} tracks with playcounts in {fetch_duration:.2f}s")
    except Exception as e:
        print(f"\n❌ Fatal error: Failed to fetch tracks from Navidrome: {str(e)}")
        print("Please check your network connection and Navidrome configuration.")
        return

    total_scrobbled = 0
    # initial_track_count = len(tracks)

    while True:
        if max_scrobbles is not None:
            s = f'{total_scrobbled} / {max_scrobbles} scrobbles'
            c = int((40 - len(s)) / 2) - 1
            o = '='*c + ' ' + s + ' '
            print(o.ljust(40, '='))
        else:
            print('='*40)

        # processed = initial_track_count - len(tracks)
        # progress_pct = (processed / initial_track_count * 100) if initial_track_count > 0 else 0
        # print(f"Progress: {processed}/{initial_track_count} tracks processed ({progress_pct:.1f}%)")

        if len(tracks) == 0:
            print("\n✅ Done. No more tracks to process.")
            break

        if max_scrobbles is not None and total_scrobbled >= max_scrobbles:
            print(f"\n🛑 Reached scrobble limit ({max_scrobbles}). Stopping.")
            break

        # get random song
        idx = random.randint(0, len(tracks) - 1)
        t = tracks[idx]

        artist = t["artist"]
        title = t["title"]
        nd_count = t["playcount"]


        # artist = 'Vildhjarta'
        # title = '+ ylva +'

        can_scrobble = True

        lf_count = get_lastfm_playcount(artist, title)
        if lf_count is None:
            can_scrobble = False
        elif lf_count >= nd_count:
            can_scrobble = False
        elif LASTFM_PLAYCOUNTS[(artist, title)]['scrobbled_this_run_count'] >= config.MAX_SCROBBLES_PER_TRACK_PER_RUN:
            can_scrobble = False
        elif lf_count >= config.MAX_SCORRBLES_PER_TRACK_TOTAL:
            can_scrobble = False

        if not can_scrobble:
            print(f"[SKIP]  {artist} – {title} | Navidrome={nd_count} Last.fm={lf_count} → no scrobble needed/possible")
            tracks.pop(idx)

        else:
            delta = nd_count - lf_count
            print(f"[MATCH] {artist} – {title} | Navidrome={nd_count} Last.fm={lf_count} → +{delta}")

            try:
                scrobble_once(artist, title, dry_run)
                total_scrobbled += 1
            except Exception as e:
                print(f"  ⚠️  Scrobble failed, will retry this track later")
                # Don't remove the track from the list, so it can be retried later
                # But continue with next track to avoid getting stuck



    total_duration = time.time() - start_time
    print(f"\n✅ Done. Total scrobbles {'planned' if dry_run else 'sent'}: {total_scrobbled}")
    print(f"Total execution time: {total_duration:.2f}s ({total_duration / 60:.2f} min)")
    if total_scrobbled > 0:
        print(f"Average time per scrobble: {total_duration / total_scrobbled:.2f}s")

# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync Navidrome playcounts into Last.fm"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not scrobble, only print actions",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Stop after N scrobbles",
    )

    args = parser.parse_args()
    main(dry_run=args.dry_run, max_scrobbles=args.max)


    # t = network.get_track('Metallica', 'The Small Hours')
    # print(t)
    # print(t.get_userplaycount())