import logging
import os

import httpx
import psycopg2
import pandas as pd

from producer.spotify_client import SpotifyClient

logger = logging.getLogger(__name__)

LASTFM_BASE = "https://ws.audioscrobbler.com/2.0/"


def get_seed_tracks(limit: int = 3) -> list[dict]:
    """Fetch top tracks from the last 7 days as recommendation seeds."""
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("PG_SPOTIFY_DB", "spotify"),
        user=os.environ.get("PG_SPOTIFY_DB_USER", "spotify-sa"),
        password=os.environ["PG_SPOTIFY_DB_USER_PASSWORD"],
    )
    try:
        df = pd.read_sql(
            """
            SELECT track_id, track_name, primary_artist_name
            FROM marts.mart_top_tracks
            ORDER BY plays_7d DESC NULLS LAST
            LIMIT %s
            """,
            conn,
            params=(limit,),
        )
        return df.to_dict(orient="records")
    finally:
        conn.close()


def _lastfm_get(params: dict) -> dict:
    api_key = os.environ.get("LASTFM_API_KEY", "")
    if not api_key:
        return {}
    try:
        resp = httpx.get(LASTFM_BASE, params={**params, "api_key": api_key, "format": "json"}, timeout=5.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Last.fm request failed: {e}")
        return {}


def get_lastfm_similar(track_name: str, artist_name: str, limit: int = 10) -> list[dict]:
    """Return similar tracks via track.getSimilar, falling back to artist.getSimilar + top tracks."""
    if not os.environ.get("LASTFM_API_KEY"):
        logger.warning("LASTFM_API_KEY not set, skipping Last.fm call")
        return []

    # try track-level similarity first
    data = _lastfm_get({"method": "track.getSimilar", "track": track_name, "artist": artist_name, "limit": limit})
    tracks = data.get("similartracks", {}).get("track", [])
    if tracks:
        return tracks

    # fallback: find similar artists, then get their top tracks
    logger.info(f"No track similarity for '{track_name}' by '{artist_name}', falling back to artist similarity")
    artist_data = _lastfm_get({"method": "artist.getSimilar", "artist": artist_name, "limit": 3})
    similar_artists = artist_data.get("similarartists", {}).get("artist", [])

    results = []
    for artist in similar_artists:
        artist_name_similar = artist.get("name", "")
        top_data = _lastfm_get({"method": "artist.getTopTracks", "artist": artist_name_similar, "limit": limit // max(len(similar_artists), 1)})
        for t in top_data.get("toptracks", {}).get("track", []):
            results.append({
                "name": t.get("name", ""),
                "artist": {"name": artist_name_similar},
                "match": float(artist.get("match", 0.5)),
                "mbid": t.get("mbid", ""),
                "url": t.get("url", ""),
            })
        if len(results) >= limit:
            break

    return results[:limit]


def get_lastfm_recommendations(seeds: list[dict], limit: int = 20) -> list[dict]:
    """Aggregate Last.fm similar tracks across seed tracks, deduplicated."""
    seen, results = set(), []
    per_seed = max(1, limit // max(len(seeds), 1))

    for seed in seeds:
        similar = get_lastfm_similar(seed["track_name"], seed["primary_artist_name"], limit=per_seed + 5)
        for item in similar:
            name = item.get("name", "")
            artist = item.get("artist", {}).get("name", "") if isinstance(item.get("artist"), dict) else item.get("artist", "")
            key = f"{artist.lower()}::{name.lower()}"
            if key not in seen:
                seen.add(key)
                results.append({
                    "track_name": name,
                    "artist_name": artist,
                    "match": float(item.get("match", 0.0)),
                    "mbid": item.get("mbid", ""),
                    "url": item.get("url", ""),
                })
            if len(results) >= limit:
                break
        if len(results) >= limit:
            break

    return results[:limit]


def get_synthetic_features_for_lastfm(tracks: list[dict]) -> dict[str, dict]:
    """Generate synthetic audio features for Last.fm tracks via artist genre lookup."""
    spotify_client = SpotifyClient()

    # SpotifyClient.get_synthetic_audio_features needs artist IDs to look up genres.
    # For Last.fm tracks, search Spotify for the artist to resolve a real artist ID.
    artist_ids: dict[str, str] = {}
    for t in tracks:
        artist_name = t["artist_name"]
        if artist_name not in artist_ids:
            try:
                results = spotify_client._sp.search(q=f"artist:{artist_name}", type="artist", limit=1)
                items = results.get("artists", {}).get("items", [])
                if items:
                    artist_ids[artist_name] = items[0]["id"]
            except Exception:
                pass

    # rebuild track objects with resolved artist IDs
    resolved = [
        {
            "id": f"lastfm::{t['artist_name']}::{t['track_name']}",
            "artists": [{"id": artist_ids.get(t["artist_name"], ""), "name": t["artist_name"]}],
        }
        for t in tracks
    ]
    features = spotify_client.get_synthetic_audio_features(resolved)
    return {f["id"]: f for f in features}
