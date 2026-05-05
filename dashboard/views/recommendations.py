import plotly.graph_objects as go
import streamlit as st

from dashboard.api_client import get_lastfm_recommendations, get_ml_recommendations
from dashboard.db import get_candidate_track_ids, get_taste_profile, get_track_metadata
from dashboard.spotify_live import _get_client

FEATURE_COLS = ["danceability", "energy", "valence",
                "acousticness", "instrumentalness", "speechiness"]


def _feature_bar(features: dict, taste: dict, title: str):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=FEATURE_COLS, y=[features.get(f, 0) for f in FEATURE_COLS],
        name="Track", marker_color="#1DB954",
    ))
    fig.add_trace(go.Bar(
        x=FEATURE_COLS, y=[taste.get(f"avg_{f}", 0) for f in FEATURE_COLS],
        name="Your Taste", marker_color="#535353",
    ))
    fig.update_layout(
        barmode="group", title=title, height=250,
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", y=-0.2),
    )
    return fig


def _add_to_playlist(track_id: str, track_name: str):
    client = _get_client()
    playlists = client._sp.current_user_playlists(limit=20).get("items", [])
    if not playlists:
        st.warning("No playlists found.")
        return

    playlist_names = [p["name"] for p in playlists]
    playlist_ids = [p["id"] for p in playlists]

    selected = st.selectbox(f"Add '{track_name}' to playlist", playlist_names,
                            key=f"playlist_select_{track_id}")
    idx = playlist_names.index(selected)

    if st.button("Confirm", key=f"confirm_{track_id}"):
        try:
            client._sp.playlist_add_items(playlist_ids[idx], [f"spotify:track:{track_id}"])
            st.success(f"Added to {selected}!")
        except Exception as e:
            st.error(f"Failed: {e}")


def render():
    st.header("Recommendations")

    taste = get_taste_profile()
    taste_dict = taste.to_dict() if not taste.empty else {}

    candidate_ids = get_candidate_track_ids(limit=20)
    track_metadata = get_track_metadata()

    col_ml, col_lastfm = st.columns(2)

    with col_ml:
        st.subheader("ML Recommendations")
        st.caption("Scored by cosine similarity to your taste profile via Feast + MLflow")

        ml_recs = get_ml_recommendations(candidate_ids, limit=10) if candidate_ids else []
        if not ml_recs:
            st.warning("No ML recommendations returned. Check that the API and feature store are running.")

        for i, rec in enumerate(ml_recs, 1):
            meta = track_metadata.get(rec["track_id"], {})
            label = f"{meta['name']} — {meta['artist']}" if meta else rec["track_id"]
            with st.expander(f"{i}. {label} — score: {rec['similarity_score']:.4f}"):
                fig = _feature_bar(rec.get("audio_features", {}), taste_dict, label)
                st.plotly_chart(fig, width='stretch')

    with col_lastfm:
        st.subheader("Last.fm Recommendations")
        st.caption("Similar tracks via Last.fm, scored against your taste profile")

        lastfm_recs = get_lastfm_recommendations(limit=10)

        if not lastfm_recs:
            st.warning("No recommendations returned. Check that the API is running.")
        else:
            for i, rec in enumerate(lastfm_recs, 1):
                track_id = rec["track_id"]
                parts = track_id.replace("lastfm::", "").split("::")
                artist = parts[0] if len(parts) > 0 else ""
                name = parts[1] if len(parts) > 1 else track_id

                with st.expander(f"{i}. {name} — {artist} — score: {rec['similarity_score']:.4f}"):
                    fig = _feature_bar(rec.get("audio_features", {}), taste_dict,
                                       f"{name} by {artist}")
                    st.plotly_chart(fig, width='stretch')

                    # Add to playlist — only works for tracks found in Spotify catalog
                    st.markdown("**Add to Spotify playlist**")
                    # Last.fm track IDs are not Spotify IDs; search Spotify first
                    if st.button("Find on Spotify & Add", key=f"search_{i}"):
                        try:
                            client = _get_client()
                            results = client._sp.search(
                                q=f"track:{name} artist:{artist}", type="track", limit=1
                            )
                            items = results.get("tracks", {}).get("items", [])
                            if items:
                                spotify_id = items[0]["id"]
                                _add_to_playlist(spotify_id, name)
                            else:
                                st.warning("Track not found on Spotify.")
                        except Exception as e:
                            st.error(f"Spotify search failed: {e}")
