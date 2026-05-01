import os 
import httpx

_BASE = os.environ.get("STREAMLIT_API_BASE_URL", "http://localhost:8000")

def get_ml_recommendations(track_ids: list[str], limit: int = 10) -> list[dict]:
    try:
        response = httpx.post(
            f"{_BASE}/recommend",
            json={"candidate_track_ids": track_ids, "limit": limit},
            timeout=10.0
        )
        response.raise_for_status()
        return response.json().get("recommendations", [])
    except Exception:
        return []
    
def get_lastfm_recommendations(limit: int = 10) -> list[dict]:
    try:
        response = httpx.get(f"{_BASE}/spotify-recommend",  params={"limit": limit}, timeout=15.0)
        response.raise_for_status()
        return response.json().get("recommendations", [])
    except Exception:
        return []