import os
from datetime import datetime, timedelta, timezone

import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
from feast import FeatureStore

FEATURE_COLS = [
    "danceability", "energy", "valence", "tempo",
    "acousticness", "instrumentalness", "speechiness", "loudness",
]
HALF_LIFE_DAYS = 30
EVAL_WINDOW_DAYS = 7
FEATURES_REPO = os.path.join(os.path.dirname(__file__), "..", "features")


def _fetch_play_history(store: FeatureStore) -> pd.DataFrame:
    """Fetch all tracks from the offline store with their audio features and play stats."""
    entity_df = _build_entity_df(store)
    df = store.get_historical_features(
        features=[
            "track_audio_features_fv:danceability",
            "track_audio_features_fv:energy",
            "track_audio_features_fv:valence",
            "track_audio_features_fv:tempo",
            "track_audio_features_fv:acousticness",
            "track_audio_features_fv:instrumentalness",
            "track_audio_features_fv:speechiness",
            "track_audio_features_fv:loudness",
            "user_track_stats_fv:play_count",
            "user_track_stats_fv:recency_score",
        ],
        entity_df=entity_df,
    ).to_df()
    # last_played_at is passed through from entity_df by Feast
    if "last_played_at" not in df.columns:
        df = df.merge(entity_df[["track_id", "last_played_at"]], on="track_id", how="left")
    return df.dropna(subset=FEATURE_COLS)


def _build_entity_df(store: FeatureStore) -> pd.DataFrame:
    """Build entity dataframe from all known tracks with a current event timestamp."""
    conn = store.config.offline_store
    import psycopg2
    pg = psycopg2.connect(
        host=conn.host, port=conn.port, dbname=conn.database,
        user=conn.user, password=os.environ["FEAST_DB_USER_PASSWORD"],
    )
    df = pd.read_sql("SELECT track_id, last_played_at FROM marts.mart_track_stats", pg)
    pg.close()
    df["event_timestamp"] = pd.Timestamp.now(tz="UTC")
    return df


def _recency_weights(last_played_at: pd.Series) -> np.ndarray:
    """Exponential decay: plays from HALF_LIFE_DAYS ago have weight 0.5."""
    now = datetime.now(tz=timezone.utc)
    days_ago = last_played_at.apply(
        lambda t: (now - t.to_pydatetime().replace(tzinfo=timezone.utc)).days
        if pd.notna(t) else HALF_LIFE_DAYS * 3
    )
    return np.exp(-np.log(2) * days_ago.values / HALF_LIFE_DAYS)


def _build_taste_vector(df: pd.DataFrame) -> np.ndarray:
    weights = _recency_weights(df["last_played_at"])
    feature_matrix = df[FEATURE_COLS].values
    weighted_sum = (feature_matrix * weights[:, None]).sum(axis=0)
    return weighted_sum / weights.sum()


class TasteProfileModel(mlflow.pyfunc.PythonModel):
    """Scores candidate tracks by cosine similarity to the user taste vector."""

    def __init__(self, taste_vector: np.ndarray, feature_cols: list[str]):
        self.taste_vector = taste_vector
        self.feature_cols = feature_cols

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        features = model_input[self.feature_cols].values
        taste_norm = self.taste_vector / (np.linalg.norm(self.taste_vector) + 1e-9)
        norms = np.linalg.norm(features, axis=1, keepdims=True) + 1e-9
        similarities = (features / norms) @ taste_norm
        return pd.DataFrame({
            "track_id": model_input["track_id"].values,
            "similarity_score": similarities,
        }).sort_values("similarity_score", ascending=False).reset_index(drop=True)


def train() -> tuple[str, np.ndarray, pd.DataFrame]:
    store = FeatureStore(repo_path=FEATURES_REPO)

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "spotify-recommendations"))

    with mlflow.start_run() as run:
        df = _fetch_play_history(store)

        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=EVAL_WINDOW_DAYS)
        train_df = df[df["last_played_at"].apply(
            lambda t: t.to_pydatetime().replace(tzinfo=timezone.utc) < cutoff
            if pd.notna(t) else True
        )]
        eval_df = df[df["last_played_at"].apply(
            lambda t: t.to_pydatetime().replace(tzinfo=timezone.utc) >= cutoff
            if pd.notna(t) else False
        )]

        taste_vector = _build_taste_vector(train_df if len(train_df) > 0 else df)
        model = TasteProfileModel(taste_vector, FEATURE_COLS)

        mlflow.log_params({
            "half_life_days": HALF_LIFE_DAYS,
            "eval_window_days": EVAL_WINDOW_DAYS,
            "train_tracks": len(train_df),
            "eval_tracks": len(eval_df),
            "feature_cols": ",".join(FEATURE_COLS),
        })
        mlflow.log_dict(
            {col: float(val) for col, val in zip(FEATURE_COLS, taste_vector)},
            "taste_vector.json",
        )

        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=model,
            registered_model_name="spotify-taste-recommender",
        )

        return run.info.run_id, taste_vector, eval_df


if __name__ == "__main__":
    run_id, taste_vector, eval_df = train()
    print(f"Run ID: {run_id}")
    print(f"Taste vector: {dict(zip(FEATURE_COLS, taste_vector))}")
    print(f"Eval tracks: {len(eval_df)}")
