import mlflow
import numpy as np
import pandas as pd

from ml.train import FEATURE_COLS, TasteProfileModel


def precision_at_k(
    model: TasteProfileModel,
    eval_df: pd.DataFrame,
    all_df: pd.DataFrame,
    k: int = 10,
) -> float:
    """Fraction of top-k recommended tracks that the user actually played in the eval period."""
    if len(eval_df) == 0 or len(all_df) == 0:
        return 0.0

    scores = model.predict(None, all_df[["track_id"] + FEATURE_COLS])
    top_k = set(scores.head(k)["track_id"])
    actually_played = set(eval_df["track_id"])
    return len(top_k & actually_played) / k


def diversity_score(model: TasteProfileModel, all_df: pd.DataFrame, k: int = 10) -> float:
    """Mean variance of audio features across top-k recommendations. Higher = more varied."""
    if len(all_df) == 0:
        return 0.0

    scores = model.predict(None, all_df[["track_id"] + FEATURE_COLS])
    top_k_ids = scores.head(k)["track_id"]
    top_k_features = all_df[all_df["track_id"].isin(top_k_ids)][FEATURE_COLS]
    return float(top_k_features.var().mean())


def evaluate(run_id: str, model: TasteProfileModel, eval_df: pd.DataFrame, all_df: pd.DataFrame) -> dict:
    metrics = {
        "precision_at_10": precision_at_k(model, eval_df, all_df, k=10),
        "precision_at_5": precision_at_k(model, eval_df, all_df, k=5),
        "diversity_score": diversity_score(model, all_df, k=10),
    }

    with mlflow.start_run(run_id=run_id):
        mlflow.log_metrics(metrics)

    return metrics


if __name__ == "__main__":
    import os
    import sys
    import numpy as np
    from feast import FeatureStore

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])

    if len(sys.argv) < 2:
        print("Usage: python -m ml.evaluate <run_id>")
        sys.exit(1)

    from ml.train import _fetch_play_history

    run_id = sys.argv[1]
    store = FeatureStore(repo_path=os.path.join(os.path.dirname(__file__), "..", "features"))
    all_df = _fetch_play_history(store)

    taste_dict = mlflow.artifacts.load_dict(run_id, "taste_vector.json")
    taste_vector = np.array([taste_dict[c] for c in FEATURE_COLS])
    model = TasteProfileModel(taste_vector, FEATURE_COLS)

    metrics = evaluate(run_id, model, all_df, all_df)
    print(metrics)
