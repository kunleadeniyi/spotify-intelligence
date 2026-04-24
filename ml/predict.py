import os

import mlflow.pyfunc
import pandas as pd
from feast import FeatureStore

from ml.train import FEATURE_COLS

FEATURES_REPO = os.path.join(os.path.dirname(__file__), "..", "features")


def _load_model():
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    return mlflow.pyfunc.load_model("models:/spotify-taste-recommender/Production")


def _fetch_candidate_features(store: FeatureStore, track_ids: list[str]) -> pd.DataFrame:
    entity_rows = [{"track_id": tid} for tid in track_ids]
    result = store.get_online_features(
        features=[f"track_audio_features_fv:{col}" for col in FEATURE_COLS],
        entity_rows=entity_rows,
    ).to_df()
    return result.dropna(subset=FEATURE_COLS)


def recommend(track_ids: list[str], limit: int = 20) -> pd.DataFrame:
    """Score and rank candidate tracks by similarity to the user's taste profile.

    Args:
        track_ids: candidate track IDs to score
        limit: number of top recommendations to return
    """
    store = FeatureStore(repo_path=FEATURES_REPO)
    model = _load_model()

    candidates = _fetch_candidate_features(store, track_ids)
    if candidates.empty:
        return pd.DataFrame(columns=["track_id", "similarity_score"])

    scores = model.predict(candidates)
    return scores.head(limit)


if __name__ == "__main__":
    import sys
    track_ids = sys.argv[1:] if len(sys.argv) > 1 else []
    if not track_ids:
        print("Usage: python -m ml.predict <track_id1> <track_id2> ...")
        sys.exit(1)

    results = recommend(track_ids)
    print(results.to_string(index=False))
