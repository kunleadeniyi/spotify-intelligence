from unittest.mock import MagicMock, patch

import pytest
import spotipy

from producer.spotify_client import SpotifyClient, _match_genre


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



MOCK_TRACKS = [
    {"id": "track_abc123", "artists": [{"id": "artist_1"}]},
    {"id": "track_def456", "artists": [{"id": "artist_2"}]},
]


class TestMatchGenre:
    def test_exact_keyword_match(self):
        assert _match_genre(["pop", "dance pop"]) == "pop"

    def test_substring_match(self):
        assert _match_genre(["dance pop"]) == "pop"

    def test_more_specific_genre_wins(self):
        # "hip-hop" appears before "pop" in _GENRE_PROFILES
        assert _match_genre(["hip-hop pop"]) == "hip-hop"

    def test_returns_default_for_unknown_genre(self):
        assert _match_genre(["zydeco experimental"]) == "default"

    def test_returns_default_for_empty_genres(self):
        assert _match_genre([]) == "default"


class TestGetArtistGenres:
    def test_returns_genre_dict(self, client):
        client._sp.artists.return_value = {
            "artists": [
                {"id": "artist_1", "genres": ["pop", "dance pop"]},
                {"id": "artist_2", "genres": ["hip-hop", "rap"]},
            ]
        }
        result = client.get_artist_genres(["artist_1", "artist_2"])
        assert result == {
            "artist_1": ["pop", "dance pop"],
            "artist_2": ["hip-hop", "rap"],
        }

    def test_returns_empty_dict_for_empty_input(self, client):
        result = client.get_artist_genres([])
        client._sp.artists.assert_not_called()
        assert result == {}

    def test_batches_over_50_artist_ids(self, client):
        client._sp.artists.return_value = {"artists": []}
        client.get_artist_genres([f"artist_{i}" for i in range(75)])
        assert client._sp.artists.call_count == 2

    def test_handles_none_artist_in_response(self, client):
        client._sp.artists.return_value = {
            "artists": [{"id": "artist_1", "genres": ["rock"]}, None]
        }
        result = client.get_artist_genres(["artist_1", "artist_bad"])
        assert "artist_1" in result
        assert None not in result


class TestGetSyntheticAudioFeatures:
    def test_returns_features_for_all_tracks(self, client):
        client._sp.artists.return_value = {
            "artists": [
                {"id": "artist_1", "genres": ["pop"]},
                {"id": "artist_2", "genres": ["rock"]},
            ]
        }
        result = client.get_synthetic_audio_features(MOCK_TRACKS)
        assert len(result) == 2
        assert result[0]["id"] == "track_abc123"
        assert result[1]["id"] == "track_def456"

    def test_synthetic_flag_is_set(self, client):
        client._sp.artists.return_value = {"artists": [{"id": "artist_1", "genres": ["pop"]}]}
        result = client.get_synthetic_audio_features([MOCK_TRACKS[0]])
        assert result[0]["synthetic"] is True

    def test_returns_empty_list_for_empty_input(self, client):
        result = client.get_synthetic_audio_features([])
        client._sp.artists.assert_not_called()
        assert result == []

    def test_falls_back_to_default_for_unknown_genre(self, client):
        client._sp.artists.return_value = {"artists": [{"id": "artist_1", "genres": ["zydeco experimental"]}]}
        result = client.get_synthetic_audio_features([MOCK_TRACKS[0]])
        assert result[0]["id"] == "track_abc123"
        assert "danceability" in result[0]

    def test_known_genre_maps_to_expected_profile(self, client):
        client._sp.artists.return_value = {"artists": [{"id": "artist_1", "genres": ["classical"]}]}
        result = client.get_synthetic_audio_features([MOCK_TRACKS[0]])
        assert result[0]["acousticness"] > 0.5

    def test_features_within_valid_range(self, client):
        client._sp.artists.return_value = {"artists": [{"id": "artist_1", "genres": ["edm"]}]}
        result = client.get_synthetic_audio_features([MOCK_TRACKS[0]])
        f = result[0]
        for field in ("danceability", "energy", "valence", "acousticness", "instrumentalness", "speechiness"):
            assert 0.0 <= f[field] <= 1.0
        assert f["tempo"] > 0
