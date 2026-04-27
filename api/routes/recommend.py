from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from api.dependencies import get_model, get_model_version, get_store
from api.models import RecommendRequest, RecommendResponse, TrackRecommendation
from api.services.recommender import fetch_candidate_features, score_candidates
from ml.train import FEATURE_COLS

router = APIRouter()


@router.post("/recommend", response_model=RecommendResponse)
def recommend(request: RecommendRequest):
    store = get_store()
    model = get_model()

    candidates = fetch_candidate_features(store, request.candidate_track_ids)
    if candidates.empty:
        raise HTTPException(status_code=404, detail="No features found for any of the provided track IDs")

    scores = score_candidates(model, candidates, request.limit)

    scores_with_features = scores.merge(candidates[["track_id"] + FEATURE_COLS], on="track_id", how="left")

    recommendations = [
        TrackRecommendation(
            track_id=row["track_id"],
            similarity_score=round(float(row["similarity_score"]), 6),
            audio_features={col: round(float(row[col]), 4) for col in FEATURE_COLS},
        )
        for _, row in scores_with_features.iterrows()
    ]

    return RecommendResponse(
        recommendations=recommendations,
        model_version=get_model_version(),
        generated_at=datetime.now(tz=timezone.utc),
        source="ml",
    )
