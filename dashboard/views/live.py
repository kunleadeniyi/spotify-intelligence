import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.db import get_current_session
from dashboard.spotify_live import get_currently_playing, get_session_genres


def render():
    st.header("Live")

    # --- Refresh controls in sidebar ---
    with st.sidebar:
        st.divider()
        auto_refresh = st.checkbox("Auto-refresh", value=True)
        interval = st.number_input("Interval (seconds)", min_value=5, max_value=300,
                                   value=30, step=5, disabled=not auto_refresh)

    # --- Trigger auto-refresh ---
    if auto_refresh:
        if "last_refresh" not in st.session_state:
            st.session_state.last_refresh = time.time()
        elapsed = time.time() - st.session_state.last_refresh
        if elapsed >= interval:
            st.session_state.last_refresh = time.time()
            st.rerun()
        else:
            remaining = int(interval - elapsed)
            st.caption(f"Refreshing in {remaining}s")
    else:
        if st.button("Refresh now"):
            st.rerun()

    # --- Layout ---
    col_now, col_session = st.columns([1, 2])

    with col_now:
        st.subheader("Now Playing")
        track = get_currently_playing()

        if not track:
            st.info("Nothing playing right now.")
        else:
            if track["album_art"]:
                st.image(track["album_art"], width=250)
            st.markdown(f"### {track['track_name']}")
            st.markdown(f"**{track['artist']}** — {track['album']}")

            progress = track["progress_ms"] / max(track["duration_ms"], 1)
            st.progress(progress)

            elapsed = f"{track['progress_ms'] // 60000}:{(track['progress_ms'] % 60000) // 1000:02d}"
            total = f"{track['duration_ms'] // 60000}:{(track['duration_ms'] % 60000) // 1000:02d}"
            st.caption(f"{elapsed} / {total}")

    with col_session:
        st.subheader("Current Session")
        session_df = get_current_session()

        if session_df.empty:
            st.info("No session data found.")
        else:
            start_time = session_df["played_at"].min()
            total_tracks = len(session_df)
            st.caption(f"Started at {start_time.strftime('%H:%M')} · {total_tracks} tracks")

            session_df["played_at_formatted"] = session_df["played_at"].dt.strftime('%Y-%m-%d %H:%M')
            session_table = session_df[["track_name", "artist", "played_at_formatted"]].rename(
                columns={"track_name": "Track", "artist": "Artist", "played_at_formatted": "Played At"}
            ).reset_index(drop=True)

            # 2. Create the Plotly Table
            fig = go.Figure(data=[go.Table(
                header=dict(
                    values=[f"<b>{col_name}</b>" for col_name in session_table.columns],
                    align='left',
                    font=dict(size=16)
                ),
                cells=dict(
                    values=[session_table[col] for col in session_table.columns],
                    align='left',
                    font=dict(size=14),
                    height=30
                )
            )])

            # 3. Force a height to enable scrolling
            fig.update_layout(
                height=370,  # Set height in pixels to trigger scrollbar
                margin=dict(l=4, r=20, t=4, b=4)
            )

            st.plotly_chart(fig, width='stretch')

        st.subheader("Session Genres")
        genres = get_session_genres()
        if genres:
            fig = go.Figure(go.Pie(
                labels=list(genres.keys())[:8],
                values=list(genres.values())[:8],
                hole=0.4,
            ))
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300,
                              showlegend=True)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No genre data for this session.")
