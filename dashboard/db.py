import os 

import psycopg2
import pandas as pd

def _connect():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("PG_SPOTIFY_DB", "spotify"),
        user=os.environ.get("PG_SPOTIFY_DB_USER"), 
        password=os.environ.get("PG_SPOTIFY_DB_USER_PASSWORD"), 
    )

def _window_col(window: str) -> str: 
    window_dict = {"7d": "plays_7d", "30d": "plays_30d", "365d": "plays_365d", "all_time": "plays_all_time"}
    return window_dict[window]

def get_top_tracks(window: str = "7d", limit: int = 10) -> pd.DataFrame:
    col = _window_col(window)
    conn = _connect()
    try:
        return pd.read_sql(
            f"SELECT track_name, primary_artist_name AS artist, {col} AS plays FROM marts.mart_top_tracks ORDER BY {col} DESC NULLS LAST LIMIT %s",
            conn, params=(limit,),
        )
    finally:
        conn.close()

def get_top_artists(window: str = "7d", limit: int = 10) -> pd.DataFrame:
    col = _window_col(window)
    conn = _connect()
    try:
        return pd.read_sql(
            f"SELECT artist_name, {col} AS plays FROM marts.mart_top_artists ORDER BY {col} DESC NULLS LAST LIMIT %s",
            conn, params=(limit,),
        )
    finally:
        conn.close()

def get_top_genres(window: str = "7d", limit: int = 10) -> pd.DataFrame:
    col = _window_col(window)
    conn = _connect()
    try:
        return pd.read_sql(
            f"SELECT genre, {col} AS plays FROM marts.mart_top_genres ORDER BY {col} DESC NULLS LAST LIMIT %s",
            conn, params=(limit,),
        )
    finally:
        conn.close()

def get_daily_stats() -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql(
            "SELECT play_date, total_plays, unique_tracks, unique_artists, total_minutes FROM marts.mart_daily_stats ORDER BY play_date DESC",
            conn,
        )
    finally:
        conn.close()

def get_heatmap() -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql(
            "SELECT day_of_week, hour_of_day, play_count FROM marts.mart_listening_heatmap",
            conn,
        )
    finally:
        conn.close()

def get_taste_profile() -> pd.Series:
    conn = _connect()
    try:
        df = pd.read_sql(
            "SELECT avg_danceability, avg_energy, avg_valence, avg_tempo, avg_acousticness, avg_instrumentalness, avg_speechiness, avg_loudness FROM marts.mart_user_taste_profile LIMIT 1",
            con=conn
        )
        return df.iloc[0] if not df.empty else pd.Series(dtype=float)
    finally:
        conn.close()

def get_current_session() -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql(
            """
            SELECT track_name, primary_artist_name AS artist, played_at
            FROM staging.int_listening_sessions
            WHERE session_id = (SELECT max(session_id) FROM staging.int_listening_sessions)
            ORDER BY played_at DESC
            """,
            conn,
        )
    finally:
        conn.close()

def get_listening_streak() -> int:
    conn = _connect()
    try:
        df = pd.read_sql(
            "SELECT play_date FROM marts.mart_daily_stats ORDER BY play_date DESC",
            conn
        )
        if df.empty:
            return 0
        today = pd.Timestamp.now().normalize()
        streak = 0
        for i, row in df.iterrows():
            expected = today  - pd.Timedelta(days=streak)
            if pd.Timestamp(row["play_date"]) == expected:
                streak += 1
            else:
                break
        return streak
    finally:
        conn.close()


def get_candidate_track_ids(limit: int = 20) -> list[str]:
    conn = _connect()
    try:
        df = pd.read_sql(
            "SELECT track_id FROM marts.mart_top_tracks ORDER BY plays_all_time DESC NULLS LAST LIMIT %s",
            conn, params=(limit,),
        )
        return df["track_id"].tolist()
    finally:
        conn.close()