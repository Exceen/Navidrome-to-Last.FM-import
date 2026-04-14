"""lastfm_stats.py

Display total Last.fm scrobble count and top 50 tracks by playcount.
"""
import pylast

import config

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


def main():
    user = network.get_user(config.LASTFM_USERNAME)

    print("Fetching all tracks from Last.fm...")
    top_items = user.get_top_tracks(period=pylast.PERIOD_OVERALL, limit=None)

    tracks = []
    total_scrobbles = 0
    for item in top_items:
        count = int(item.weight)
        total_scrobbles += count
        tracks.append({
            "artist": str(item.item.artist),
            "title": str(item.item.title),
            "playcount": count,
        })

    print(f"\nTotal unique tracks: {len(tracks)}")
    print(f"Total scrobbles:     {total_scrobbles}\n")

    print("=== Top 50 tracks ===\n")
    print(f"{'#':<5} {'Plays':<8} {'Artist':<30} {'Title'}")
    print("-" * 80)
    for i, t in enumerate(tracks[:50], 1):
        print(f"{i:<5} {t['playcount']:<8} {t['artist']:<30} {t['title']}")


if __name__ == "__main__":
    main()
