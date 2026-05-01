import datetime

from producer.spotify_client import SpotifyClient

_client: SpotifyClient | None = None


def _get_client() -> SpotifyClient:
    global _client
    if _client is None:
        _client = SpotifyClient()
    return _client


def get_currently_playing() -> dict | None:
    result = _get_client().get_currently_playing()
    if not result:
        return None
    item = result["item"]
    return {
        "track_name": item["name"],
        "artist": ", ".join(a["name"] for a in item["artists"]),
        "album": item["album"]["name"],
        "album_art": item["album"]["images"][0]["url"] if item["album"]["images"] else None,
        "progress_ms": result.get("progress_ms", 0),
        "duration_ms": item["duration_ms"],
        "track_id": item["id"],
    }


def get_session_genres() -> dict[str, int]:
    """Genre counts for the most recent session's tracks."""
    client = _get_client()
    recent = client.get_recently_played(limit=20)
    if not recent:
        return {}

    # find the most recent session boundary (30-min gap)
    session_tracks = []
    for i, item in enumerate(recent):
        if i == 0:
            session_tracks.append(item)
            continue
        prev_ts = recent[i - 1]["played_at"]
        curr_ts = item["played_at"]
        prev_dt = datetime.datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
        curr_dt = datetime.datetime.fromisoformat(curr_ts.replace("Z", "+00:00"))
        if (prev_dt - curr_dt).total_seconds() > 1800:
            break
        session_tracks.append(item)

    artist_ids = list({t["track"]["artists"][0]["id"] for t in session_tracks if t["track"]["artists"]})
    genre_map = client.get_artist_genres(artist_ids)

    counts: dict[str, int] = {}
    for genres in genre_map.values():
        for g in genres:
            counts[g] = counts.get(g, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))
