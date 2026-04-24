# ML — Taste-Based Recommendation Model

A cosine similarity recommender that ranks candidate tracks by how closely they match the user's listening taste.

## How it works

**1. Taste profile**
Reads audio features (danceability, energy, valence, tempo, acousticness, instrumentalness, speechiness, loudness) for every track in the user's play history and computes a recency-weighted average. Plays from the last 30 days carry full weight; older plays decay exponentially with a 30-day half-life.

**2. Scoring**
At serving time, fetches each candidate track's audio features from the Feast online store (Redis) and computes cosine similarity against the taste vector. Returns candidates ranked highest to lowest.

## Files

| File | Purpose |
|---|---|
| `train.py` | Fetches offline features via Feast, builds the taste vector, registers the model in MLflow |
| `evaluate.py` | Computes precision@k and diversity score against a held-out eval window |
| `predict.py` | Loads the Production model from MLflow, scores candidate tracks from Redis |

## Running locally

```bash
# Train and register a new model version
python -m ml.train

# Evaluate a specific run
python -m ml.evaluate <run_id>

# Score candidate tracks
python -m ml.predict <track_id1> <track_id2> ...
```

## MLflow

Experiments and model versions are tracked at `MLFLOW_TRACKING_URI` (default: `http://localhost:5555`).
The training flow promotes a new version to `Production` only if its `precision_at_10` beats the current Production model.

## Retraining

The Prefect flow `model-training-weekly` runs the full train → evaluate → conditional promotion pipeline on a weekly schedule via the `local-pool` worker.
