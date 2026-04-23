import subprocess
from datetime import timedelta
from pathlib import Path

from prefect import flow, get_run_logger

FEATURES_DIR = str(Path(__file__).parents[2] / "features")


@flow(
    name="feast-materialization-flow",
    description="Materializes Feast features from Postgres offline store to Redis online store",
)
def run_materialization() -> None:
    logger = get_run_logger()
    logger.info("Starting Feast materialization", extra={"features_dir": FEATURES_DIR})

    result = subprocess.run(
        ["python", "materialize.py"],
        cwd=FEATURES_DIR,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    logger.info(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Feast materialization failed:\n{result.stderr}")


if __name__ == "__main__":
    run_materialization.from_source(
        source="/app",
        entrypoint="orchestration/flows/feature_flow.py:run_materialization",
    ).deploy(
        name="feast-materialization-6h",
        work_pool_name="local-pool",
        interval=timedelta(hours=6),
    )
