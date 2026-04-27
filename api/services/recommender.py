import logging

import pandas as pd
from feast import FeatureStore

from ml.train import FEATURE_COLS

logger = logging.getLogger(__name__)


def fetch_candidate_features(store: FeatureStore, track_ids: list[str]) -> pd.DataFrame:
    entity_rows = [{"track_id": tid} for tid in track_ids]
    result = store.get_online_features(
        features=[f"track_audio_features_fv:{col}" for col in FEATURE_COLS],
        entity_rows=entity_rows,
    ).to_df()

    missing = result[result[FEATURE_COLS].isnull().any(axis=1)]["track_id"].tolist()
    if missing:
        logger.warning(f"Missing features for {len(missing)} tracks, dropping: {missing}")

    return result.dropna(subset=FEATURE_COLS)


def score_candidates(model, candidates: pd.DataFrame, limit: int) -> pd.DataFrame:
    scores = model.predict(candidates)
    return scores.head(limit)
