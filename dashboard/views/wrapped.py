import plotly.graph_objects as go
import streamlit as st

from dashboard.db import (
    get_daily_stats,
    get_heatmap,
    get_listening_streak,
    get_taste_profile,
    get_top_artists,
    get_top_genres,
    get_top_tracks,
)

WINDOW_OPTIONS = {"7 days": "7d", "30 days": "30d", "12 months": "365d", "All time": "all_time"}

RADAR_FEATURES = ["avg_danceability", "avg_energy", "avg_valence",
                  "avg_acousticness", "avg_instrumentalness", "avg_speechiness"]
RADAR_LABELS = ["Danceability", "Energy", "Valence",
                "Acousticness", "Instrumentalness", "Speechiness"]


def render():
    st.header("Wrapped")

    window_label = st.selectbox("Time range", list(WINDOW_OPTIONS.keys()))
    window = WINDOW_OPTIONS[window_label]

    # --- Top Tracks ---
    st.subheader("Top Tracks")
    tracks = get_top_tracks(window=window, limit=10)
    if tracks.empty:
        st.info("No play data yet.")
    else:
        tracks.index = range(1, len(tracks) + 1)
        st.table(tracks)

    st.divider()

    # --- Top Artists & Top Genres side by side ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Artists")
        artists = get_top_artists(window=window, limit=10)
        if not artists.empty:
            artists.index = range(1, len(artists) + 1)
            st.table(artists)

    with col2:
        st.subheader("Top Genres")
        genres = get_top_genres(window=window, limit=10)
        if not genres.empty:
            fig = go.Figure(go.Bar(
                x=genres["plays"],
                y=genres["genre"],
                orientation="h",
                marker_color="#1DB954",
            ))
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=350,
                              yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, width='stretch')

    st.divider()

    # --- Listening Heatmap ---
    st.subheader("Listening Heatmap")
    heatmap_df = get_heatmap()
    if not heatmap_df.empty:
        pivot = heatmap_df.pivot_table(
            index="day_of_week", columns="hour_of_day", values="play_count", fill_value=0
        )
        pivot = pivot.reindex(index=range(7), columns=range(24), fill_value=0)
        day_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        fig = go.Figure(go.Heatmap(
            z=pivot.values,
            x=[f"{h:02d}:00" for h in range(24)],
            y=[day_labels[i] for i in pivot.index],
            colorscale="Viridis",
            showscale=True,
        ))
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=250)
        st.plotly_chart(fig, width='stretch')

    st.divider()

    # --- Streak & Daily Stats ---
    col3, col4 = st.columns([1, 3])

    with col3:
        streak = get_listening_streak()
        st.metric("Listening Streak", f"{streak} days")

    with col4:
        st.subheader("Daily Plays (last 30 days)")
        daily = get_daily_stats().head(30)
        if not daily.empty:
            daily = daily.sort_values("play_date")
            fig = go.Figure(go.Scatter(
                x=daily["play_date"], y=daily["total_plays"],
                mode="lines+markers", line_color="#1DB954",
            ))
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=200)
            st.plotly_chart(fig, width='stretch')

    st.divider()

    # --- Taste Radar ---
    st.subheader("Your Taste Profile")
    profile = get_taste_profile()
    if not profile.empty:
        values = [float(profile.get(f, 0)) for f in RADAR_FEATURES]
        fig = go.Figure(go.Scatterpolar(
            r=values + [values[0]],
            theta=RADAR_LABELS + [RADAR_LABELS[0]],
            fill="toself",
            line_color="#1DB954",
        ))
        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            margin=dict(l=40, r=40, t=40, b=40),
            height=400,
        )
        st.plotly_chart(fig, width='stretch')
