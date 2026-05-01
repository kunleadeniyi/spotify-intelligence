import logging
from datetime import datetime, timezone

import numpy as np
from fastapi import APIRouter, HTTPException

from api.dependencies import get_model_version, get_taste_vector
from api.metrics import recommendations_served
from api.models import RecommendResponse, TrackRecommendation
from api.services.spotify_svc import (
    get_lastfm_recommendations,
    get_seed_tracks,
    get_synthetic_features_for_lastfm,
)
from ml.train import FEATURE_COLS

logger = logging.getLogger(__name__)

router = APIRouter()


def _cosine_similarity(vec_a: np.ndarray, vec_b: np.ndarray) -> float:
    norm_a = np.linalg.norm(vec_a) + 1e-9
    norm_b = np.linalg.norm(vec_b) + 1e-9
    return float(np.dot(vec_a, vec_b) / (norm_a * norm_b))


@router.get("/spotify-recommend", response_model=RecommendResponse)
def spotify_recommend(limit: int = 20):
    seeds = get_seed_tracks(limit=3)
    if not seeds:
        raise HTTPException(status_code=404, detail="No play history found to generate seed tracks")

    tracks = get_lastfm_recommendations(seeds, limit=limit)
    if not tracks:
        raise HTTPException(status_code=502, detail="Last.fm returned no similar tracks")

    taste = get_taste_vector()
    features_map = get_synthetic_features_for_lastfm(tracks)

    recommendations = []
    for track in tracks:
        track_key = f"lastfm::{track['artist_name']}::{track['track_name']}"
        raw = features_map.get(track_key)
        if not raw:
            continue

        features = {col: float(raw.get(col, 0.0)) for col in FEATURE_COLS}
        feature_vec = np.array([features[col] for col in FEATURE_COLS])
        score = _cosine_similarity(taste, feature_vec)

        recommendations.append(
            TrackRecommendation(
                track_id=track_key,
                similarity_score=round(score, 6),
                audio_features={k: round(v, 4) for k, v in features.items()},
            )
        )

    recommendations.sort(key=lambda r: r.similarity_score, reverse=True)
    recommendations_served.labels(source="lastfm").inc(len(recommendations))

    return RecommendResponse(
        recommendations=recommendations[:limit],
        model_version=get_model_version(),
        generated_at=datetime.now(tz=timezone.utc),
        source="lastfm",
    )
