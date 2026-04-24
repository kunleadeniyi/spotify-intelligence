import os
from datetime import timedelta

import mlflow
from prefect import flow, get_run_logger

from ml.train import FEATURE_COLS, TasteProfileModel, _fetch_play_history, train
from ml.evaluate import evaluate


def _get_production_metrics() -> dict | None:
    """Return metrics of the current Production model, or None if none exists."""
    client = mlflow.MlflowClient()
    try:
        versions = client.get_latest_versions("spotify-taste-recommender", stages=["Production"])
        if not versions:
            return None
        run = client.get_run(versions[0].run_id)
        return run.data.metrics
    except Exception:
        return None


def _promote_to_production(run_id: str) -> None:
    client = mlflow.MlflowClient()
    versions = client.get_latest_versions("spotify-taste-recommender", stages=["None"])
    # find the version registered from this run
    for v in versions:
        if v.run_id == run_id:
            client.transition_model_version_stage(
                name="spotify-taste-recommender",
                version=v.version,
                stage="Production",
                archive_existing_versions=True,
            )
            return


@flow(
    name="model-training-flow",
    description="Trains recommendation model, evaluates it, and promotes to Production if metrics improve",
)
def run_training() -> None:
    logger = get_run_logger()

    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "spotify-recommendations"))

    logger.info("Fetching play history and training model")
    run_id, taste_vector, eval_df = train()

    from feast import FeatureStore
    store = FeatureStore(repo_path="/app/features")
    all_df = _fetch_play_history(store)
    model = TasteProfileModel(taste_vector, FEATURE_COLS)

    logger.info("Evaluating model")
    metrics = evaluate(run_id, model, eval_df, all_df)
    logger.info(f"Metrics: {metrics}")

    current = _get_production_metrics()
    new_p10 = metrics.get("precision_at_10", 0.0)
    current_p10 = current.get("precision_at_10", 0.0) if current else 0.0

    if current is None or new_p10 >= current_p10:
        logger.info(f"Promoting model (precision@10: {current_p10:.4f} → {new_p10:.4f})")
        _promote_to_production(run_id)
    else:
        logger.info(f"Keeping current model (precision@10: {current_p10:.4f} > {new_p10:.4f})")


if __name__ == "__main__":
    run_training.from_source(
        source="/app",
        entrypoint="orchestration/flows/training_flow.py:run_training",
    ).deploy(
        name="model-training-weekly",
        work_pool_name="local-pool",
        interval=timedelta(weeks=1),
    )
