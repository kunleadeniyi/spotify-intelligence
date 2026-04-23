import os
import subprocess
from datetime import timedelta
from pathlib import Path

from prefect import flow, get_run_logger

DBT_DIR = str(Path(os.environ.get("DBT_PROJECT_DIR", Path(__file__).parents[2] / "dbt")))
DBT_PROFILES_DIR = str(Path(os.environ.get("DBT_PROFILES_DIR", Path(__file__).parents[2] / "dbt" / "profiles")))


@flow(
    name="dbt-transformation-flow",
    description="Runs dbt models and tests on an hourly schedule",
)
def run_dbt() -> None:
    logger = get_run_logger()
    logger.info("Starting dbt build", extra={"dbt_dir": DBT_DIR})

    result = subprocess.run(
        ["dbt", "build", "--profiles-dir", DBT_PROFILES_DIR],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt build failed:\n{result.stderr}")

if __name__ == "__main__":
    run_dbt.from_source(
        source="/app",
        entrypoint="orchestration/flows/dbt_flow.py:run_dbt",
    ).deploy(
        name="dbt-hourly",
        work_pool_name="local-pool",
        interval=timedelta(hours=1),
    )