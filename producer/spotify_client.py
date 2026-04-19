import os
import random
import logging
from typing import Optional

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

SCOPES = "user-read-currently-playing user-read-recently-played"

# Genre keyword → baseline audio feature profile.
# Keys are checked against Spotify genre tags via substring match (e.g. "indie rock" matches "rock").
# More specific genres are listed first so they take priority.
_GENRE_PROFILES: dict[str, dict] = {
    # Electronic
    "edm":         {"danceability": 0.82, "energy": 0.88, "valence": 0.62, "acousticness": 0.02, "instrumentalness": 0.45, "speechiness": 0.06, "loudness": -5.0,  "tempo": 128.0},
    "electronic":  {"danceability": 0.75, "energy": 0.82, "valence": 0.55, "acousticness": 0.04, "instrumentalness": 0.55, "speechiness": 0.05, "loudness": -6.0,  "tempo": 122.0},
    # Hip-hop / Rap
    "hip-hop":     {"danceability": 0.80, "energy": 0.68, "valence": 0.52, "acousticness": 0.12, "instrumentalness": 0.02, "speechiness": 0.28, "loudness": -6.5,  "tempo": 94.0},
    "rap":         {"danceability": 0.78, "energy": 0.70, "valence": 0.48, "acousticness": 0.10, "instrumentalness": 0.01, "speechiness": 0.32, "loudness": -6.0,  "tempo": 96.0},
    # Afro variants — compound keys must precede their base keys (afropop before pop, afro r&b before r&b, etc.)
    "afroswing":   {"danceability": 0.82, "energy": 0.72, "valence": 0.68, "acousticness": 0.12, "instrumentalness": 0.06, "speechiness": 0.15, "loudness": -6.0,  "tempo": 100.0},
    "afropop":     {"danceability": 0.85, "energy": 0.78, "valence": 0.80, "acousticness": 0.15, "instrumentalness": 0.04, "speechiness": 0.08, "loudness": -5.5,  "tempo": 108.0},
    "afrogospel":  {"danceability": 0.72, "energy": 0.70, "valence": 0.78, "acousticness": 0.35, "instrumentalness": 0.04, "speechiness": 0.10, "loudness": -6.5,  "tempo": 100.0},
    "afrobeats":   {"danceability": 0.88, "energy": 0.74, "valence": 0.78, "acousticness": 0.18, "instrumentalness": 0.05, "speechiness": 0.12, "loudness": -6.0,  "tempo": 102.0},
    "afrobeat":    {"danceability": 0.72, "energy": 0.70, "valence": 0.65, "acousticness": 0.25, "instrumentalness": 0.35, "speechiness": 0.10, "loudness": -7.0,  "tempo": 105.0},
    "afro r&b":    {"danceability": 0.76, "energy": 0.62, "valence": 0.65, "acousticness": 0.28, "instrumentalness": 0.04, "speechiness": 0.09, "loudness": -7.0,  "tempo": 98.0},
    "afro soul":   {"danceability": 0.68, "energy": 0.55, "valence": 0.62, "acousticness": 0.42, "instrumentalness": 0.06, "speechiness": 0.08, "loudness": -8.5,  "tempo": 92.0},
    "highlife":    {"danceability": 0.75, "energy": 0.65, "valence": 0.76, "acousticness": 0.40, "instrumentalness": 0.15, "speechiness": 0.07, "loudness": -8.0,  "tempo": 105.0},
    "alté":        {"danceability": 0.65, "energy": 0.58, "valence": 0.52, "acousticness": 0.32, "instrumentalness": 0.12, "speechiness": 0.10, "loudness": -7.5,  "tempo": 108.0},
    # Reggae
    "reggae":      {"danceability": 0.80, "energy": 0.58, "valence": 0.72, "acousticness": 0.25, "instrumentalness": 0.08, "speechiness": 0.10, "loudness": -8.0,  "tempo": 78.0},
    # Gospel / Christian
    "gospel":      {"danceability": 0.60, "energy": 0.65, "valence": 0.75, "acousticness": 0.45, "instrumentalness": 0.04, "speechiness": 0.08, "loudness": -7.0,  "tempo": 95.0},
    "worship":     {"danceability": 0.42, "energy": 0.48, "valence": 0.68, "acousticness": 0.58, "instrumentalness": 0.06, "speechiness": 0.05, "loudness": -9.5,  "tempo": 85.0},
    "christian":   {"danceability": 0.55, "energy": 0.60, "valence": 0.70, "acousticness": 0.42, "instrumentalness": 0.05, "speechiness": 0.06, "loudness": -8.0,  "tempo": 92.0},
    # Classical / Jazz / Blues
    "classical":   {"danceability": 0.25, "energy": 0.22, "valence": 0.32, "acousticness": 0.92, "instrumentalness": 0.88, "speechiness": 0.04, "loudness": -16.0, "tempo": 82.0},
    "jazz":        {"danceability": 0.48, "energy": 0.38, "valence": 0.55, "acousticness": 0.78, "instrumentalness": 0.42, "speechiness": 0.05, "loudness": -12.0, "tempo": 112.0},
    "blues":       {"danceability": 0.52, "energy": 0.50, "valence": 0.45, "acousticness": 0.55, "instrumentalness": 0.12, "speechiness": 0.05, "loudness": -10.0, "tempo": 88.0},
    # Soul / R&B / Funk — base keys after afro variants
    "soul":        {"danceability": 0.65, "energy": 0.55, "valence": 0.60, "acousticness": 0.45, "instrumentalness": 0.05, "speechiness": 0.08, "loudness": -8.0,  "tempo": 95.0},
    "r&b":         {"danceability": 0.72, "energy": 0.60, "valence": 0.55, "acousticness": 0.22, "instrumentalness": 0.03, "speechiness": 0.10, "loudness": -7.0,  "tempo": 96.0},
    "funk":        {"danceability": 0.82, "energy": 0.72, "valence": 0.70, "acousticness": 0.20, "instrumentalness": 0.18, "speechiness": 0.08, "loudness": -7.0,  "tempo": 108.0},
    # Rock
    "metal":       {"danceability": 0.35, "energy": 0.95, "valence": 0.28, "acousticness": 0.02, "instrumentalness": 0.45, "speechiness": 0.06, "loudness": -4.0,  "tempo": 148.0},
    "punk":        {"danceability": 0.55, "energy": 0.88, "valence": 0.52, "acousticness": 0.04, "instrumentalness": 0.08, "speechiness": 0.07, "loudness": -5.5,  "tempo": 158.0},
    "rock":        {"danceability": 0.52, "energy": 0.82, "valence": 0.48, "acousticness": 0.08, "instrumentalness": 0.15, "speechiness": 0.05, "loudness": -6.0,  "tempo": 130.0},
    "indie":       {"danceability": 0.55, "energy": 0.62, "valence": 0.48, "acousticness": 0.28, "instrumentalness": 0.10, "speechiness": 0.05, "loudness": -8.0,  "tempo": 118.0},
    "folk":        {"danceability": 0.45, "energy": 0.40, "valence": 0.50, "acousticness": 0.72, "instrumentalness": 0.08, "speechiness": 0.05, "loudness": -12.0, "tempo": 98.0},
    "country":     {"danceability": 0.60, "energy": 0.65, "valence": 0.62, "acousticness": 0.45, "instrumentalness": 0.02, "speechiness": 0.05, "loudness": -7.0,  "tempo": 122.0},
    # Pop / Latin — after afropop
    "latin":       {"danceability": 0.82, "energy": 0.72, "valence": 0.74, "acousticness": 0.22, "instrumentalness": 0.05, "speechiness": 0.08, "loudness": -6.5,  "tempo": 108.0},
    "pop":         {"danceability": 0.68, "energy": 0.65, "valence": 0.58, "acousticness": 0.18, "instrumentalness": 0.02, "speechiness": 0.06, "loudness": -6.5,  "tempo": 118.0},
    "default":     {"danceability": 0.55, "energy": 0.55, "valence": 0.50, "acousticness": 0.20, "instrumentalness": 0.05, "speechiness": 0.06, "loudness": -8.0,  "tempo": 115.0},
}

_BOUNDED_FIELDS = {"danceability", "energy", "valence", "acousticness", "instrumentalness", "speechiness"}


def _match_genre(genres: list[str]) -> str:
    """Return the first _GENRE_PROFILES key found as a substring in any genre tag."""
    for profile_key in _GENRE_PROFILES:
        if profile_key == "default":
            continue
        for tag in genres:
            if profile_key in tag.lower():
                return profile_key
    return "default"


class SpotifyClient:
    def __init__(self) -> None:
        self._sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                client_id=os.environ["SPOTIFY_CLIENT_ID"],
                client_secret=os.environ["SPOTIFY_CLIENT_SECRET"],
                redirect_uri=os.environ["SPOTIFY_REDIRECT_URI"],
                scope=SCOPES,
                cache_path=os.environ.get("SPOTIFY_TOKEN_CACHE", ".cache"),
                open_browser=False,
            )
        )

    @retry(
        retry=retry_if_exception_type(spotipy.SpotifyException),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_currently_playing(self) -> Optional[dict]:
        result = self._sp.current_user_playing_track()
        if not result or not result.get("item"):
            return None
        return result

    @retry(
        retry=retry_if_exception_type(spotipy.SpotifyException),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_recently_played(self, limit: int = 50) -> list[dict]:
        result = self._sp.current_user_recently_played(limit=limit)
        return result.get("items", [])

    @retry(
        retry=retry_if_exception_type(spotipy.SpotifyException),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_artist_genres(self, artist_ids: list[str]) -> dict[str, list[str]]:
        """Fetch genre tags for a list of artist IDs. Returns {artist_id: [genres]}."""
        if not artist_ids:
            return {}
        result: dict[str, list[str]] = {}
        for i in range(0, len(artist_ids), 50):
            batch = artist_ids[i : i + 50]
            response = self._sp.artists(batch)
            for artist in response.get("artists", []):
                if artist:
                    result[artist["id"]] = artist.get("genres", [])
        return result

    def get_synthetic_audio_features(self, tracks: list[dict]) -> list[dict]:
        """Generate plausible audio features from artist genre tags.

        Replaces the deprecated /v1/audio-features endpoint. Features are
        derived from genre-based profiles with per-track random jitter so
        tracks within the same genre are not identical.
        """
        if not tracks:
            return []

        artist_ids = list({
            artist["id"]
            for track in tracks
            for artist in track.get("artists", [])
        })
        artist_genres = self.get_artist_genres(artist_ids)

        results = []
        for track in tracks:
            first_artist_id = (track.get("artists") or [{}])[0].get("id", "")
            genres = artist_genres.get(first_artist_id, [])
            profile_key = _match_genre(genres)
            features = _GENRE_PROFILES[profile_key].copy()

            for field in _BOUNDED_FIELDS:
                features[field] = round(
                    min(1.0, max(0.0, features[field] + random.uniform(-0.05, 0.05))), 4
                )
            features["tempo"] = round(features["tempo"] + random.uniform(-10, 10), 3)
            features["loudness"] = round(features["loudness"] + random.uniform(-1, 1), 3)

            results.append({"id": track["id"], "synthetic": True, **features})

        return results
