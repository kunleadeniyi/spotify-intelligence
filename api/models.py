from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RecommendRequest(BaseModel):
    candidate_track_ids: list[str] = Field(..., min_length=1, max_length=100)
    limit: int = Field(default=20, ge=1, le=100)


class TrackRecommendation(BaseModel):
    track_id: str
    similarity_score: float
    audio_features: dict[str, float]


class RecommendResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    recommendations: list[TrackRecommendation]
    model_version: str
    generated_at: datetime
    source: Literal["ml", "lastfm"]
