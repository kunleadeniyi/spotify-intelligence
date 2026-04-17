import os
import logging
from typing import Optional

import spotipy
from spotipy.oauth2 import SpotifyOAuth
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

SCOPES = "user-read-currently-playing user-read-recently-played"


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
    def get_audio_features(self, track_ids: list[str]) -> list[dict]:
        if not track_ids:
            return []
        results = []
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i : i + 100]
            features = self._sp.audio_features(batch)
            results.extend([f for f in features if f is not None])
        return results
