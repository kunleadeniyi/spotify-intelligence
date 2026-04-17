from unittest.mock import MagicMock, patch

import pytest
import spotipy

from producer.spotify_client import SpotifyClient


MOCK_CURRENTLY_PLAYING = {
    "is_playing": True,
    "item": {
        "id": "track_abc123",
        "name": "Blinding Lights",
        "artists": [{"id": "artist_1", "name": "The Weeknd"}],
        "album": {"name": "After Hours"},
        "duration_ms": 200040,
    },
    "progress_ms": 45000,
}

MOCK_RECENTLY_PLAYED = {
    "items": [
        {
            "track": {
                "id": "track_abc123",
                "name": "Blinding Lights",
                "artists": [{"id": "artist_1", "name": "The Weeknd"}],
            },
            "played_at": "2024-01-01T12:00:00Z",
        },
        {
            "track": {
                "id": "track_def456",
                "name": "Save Your Tears",
                "artists": [{"id": "artist_1", "name": "The Weeknd"}],
            },
            "played_at": "2024-01-01T11:45:00Z",
        },
    ]
}

MOCK_AUDIO_FEATURES = [
    {
        "id": "track_abc123",
        "danceability": 0.514,
        "energy": 0.73,
        "tempo": 171.005,
        "valence": 0.334,
        "acousticness": 0.00146,
        "instrumentalness": 0.0000224,
        "speechiness": 0.0598,
        "loudness": -5.934,
    },
    {
        "id": "track_def456",
        "danceability": 0.673,
        "energy": 0.813,
        "tempo": 118.051,
        "valence": 0.649,
        "acousticness": 0.00253,
        "instrumentalness": 0.0,
        "speechiness": 0.0424,
        "loudness": -5.49,
    },
]


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("SPOTIFY_CLIENT_ID", "test_client_id")
    monkeypatch.setenv("SPOTIFY_CLIENT_SECRET", "test_client_secret")
    monkeypatch.setenv("SPOTIFY_REDIRECT_URI", "http://localhost:8888/callback")

    with patch("producer.spotify_client.SpotifyOAuth"), \
         patch("producer.spotify_client.spotipy.Spotify") as mock_spotify:
        mock_spotify.return_value = MagicMock()
        c = SpotifyClient()
        c._sp = mock_spotify.return_value
        yield c


class TestGetCurrentlyPlaying:
    def test_returns_track_when_playing(self, client):
        client._sp.current_user_playing_track.return_value = MOCK_CURRENTLY_PLAYING
        result = client.get_currently_playing()
        assert result == MOCK_CURRENTLY_PLAYING

    def test_returns_none_when_nothing_playing(self, client):
        client._sp.current_user_playing_track.return_value = None
        result = client.get_currently_playing()
        assert result is None

    def test_returns_none_when_no_item(self, client):
        client._sp.current_user_playing_track.return_value = {"is_playing": False, "item": None}
        result = client.get_currently_playing()
        assert result is None

    def test_retries_on_rate_limit(self, client):
        rate_limited = spotipy.SpotifyException(http_status=429, code=-1, msg="rate limited")
        client._sp.current_user_playing_track.side_effect = [
            rate_limited,
            MOCK_CURRENTLY_PLAYING,
        ]
        result = client.get_currently_playing()
        assert result == MOCK_CURRENTLY_PLAYING
        assert client._sp.current_user_playing_track.call_count == 2


class TestGetRecentlyPlayed:
    def test_returns_items(self, client):
        client._sp.current_user_recently_played.return_value = MOCK_RECENTLY_PLAYED
        result = client.get_recently_played()
        assert len(result) == 2
        assert result[0]["track"]["id"] == "track_abc123"

    def test_returns_empty_list_when_no_items(self, client):
        client._sp.current_user_recently_played.return_value = {"items": []}
        result = client.get_recently_played()
        assert result == []

    def test_passes_limit(self, client):
        client._sp.current_user_recently_played.return_value = MOCK_RECENTLY_PLAYED
        client.get_recently_played(limit=10)
        client._sp.current_user_recently_played.assert_called_once_with(limit=10)


class TestGetAudioFeatures:
    def test_returns_features(self, client):
        client._sp.audio_features.return_value = MOCK_AUDIO_FEATURES
        result = client.get_audio_features(["track_abc123", "track_def456"])
        assert len(result) == 2
        assert result[0]["id"] == "track_abc123"

    def test_returns_empty_list_for_empty_input(self, client):
        result = client.get_audio_features([])
        client._sp.audio_features.assert_not_called()
        assert result == []

    def test_filters_out_none_features(self, client):
        client._sp.audio_features.return_value = [MOCK_AUDIO_FEATURES[0], None]
        result = client.get_audio_features(["track_abc123", "track_bad"])
        assert len(result) == 1

    def test_batches_over_100_track_ids(self, client):
        track_ids = [f"track_{i}" for i in range(150)]
        client._sp.audio_features.return_value = []
        client.get_audio_features(track_ids)
        assert client._sp.audio_features.call_count == 2
        first_batch = client._sp.audio_features.call_args_list[0][0][0]
        second_batch = client._sp.audio_features.call_args_list[1][0][0]
        assert len(first_batch) == 100
        assert len(second_batch) == 50
