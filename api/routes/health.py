from fastapi import APIRouter

from api.dependencies import get_model_version

router = APIRouter()


@router.get("/health")
def health():
    return {"status": "ok", "model_version": get_model_version()}
