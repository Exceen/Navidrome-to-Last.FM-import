import requests
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
TEST_MODE = False

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
if TEST_MODE:
    NAVIDROME_PAGE_COUNT = 10
    BREAK_AT_FIRST_PAGE = True

# ----------------------------
# Helpers
# ----------------------------

def normalize(s):
    return s.lower().strip()

def fuzzy_match(a, b):
    return fuzz.token_sort_ratio(normalize(a), normalize(b))

def find_track(artist, title):
    try:
        track = network.get_track(artist, title)
        track.get_userplaycount() # verify it exists
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
                    track.get_userplaycount() # verify it exists
                    print('  -> matched!', '|', track.artist, '|', track.title, '|')
                    return track
                except WSError:
                    continue

    except Exception as e:
        print('Exception finding track:', e)

    return None


LASTFM_PLAYCOUNTS = {}
def get_lastfm_playcount(artist, title):
    if not((artist, title) in LASTFM_PLAYCOUNTS):
        track = find_track(artist, title)
        if track is None:
            LASTFM_PLAYCOUNTS[(artist, title)] = {}
        else:
            LASTFM_PLAYCOUNTS[(artist, title)] = {
                'playcount': track.get_userplaycount() or 0,
                'scrobbled_this_run_count': 0,
            }

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
        network.scrobble(
            artist=artist,
            title=title,
            timestamp=timestamp,
        )
        time.sleep(config.SCROBBLE_DELAY)

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
    r = requests.get(
        f"{config.NAVIDROME_API_URL}/{endpoint}",
        params={**base, **params},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["subsonic-response"]

def fetch_all_tracks():
    """
    Fetches all albums, then all songs per album.
    Returns [{artist, title, playcount}]
    """
    tracks = []

    offset = 0
    size = NAVIDROME_PAGE_COUNT

    while True:
        resp = api_call(
            "getAlbumList2.view",
            {"type": "alphabeticalByName", "offset": offset, "size": size},
        )

        albums = resp.get("albumList2", {}).get("album", [])
        if not albums:
            break

        for album in albums:
            album_id = album["id"]
            album_resp = api_call("getAlbum.view", {"id": album_id})
            songs = album_resp.get("album", {}).get("song", [])

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

        offset += len(albums)
        if len(albums) < size:
            break

        if BREAK_AT_FIRST_PAGE:
            break

    return tracks

# ----------------------------
# Main logic
# ----------------------------
def main(dry_run, max_scrobbles):
    if not dry_run:
        print("⚠️  LIVE MODE: scrobbles will be sent to Last.fm\n")
    else:
        print("🧪 DRY-RUN MODE: no scrobbles will be sent\n")
    
    tracks = fetch_all_tracks()
    print(f"Fetched {len(tracks)} tracks with playcounts")

    total_scrobbled = 0

    while True:
        print('='*40)
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

            scrobble_once(artist, title, dry_run)
            total_scrobbled += 1



    print(f"\n✅ Done. Total scrobbles {'planned' if dry_run else 'sent'}: {total_scrobbled}")

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