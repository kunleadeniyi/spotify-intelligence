from datetime import datetime, timezone

from feast import FeatureStore


def materialize():
    store = FeatureStore(repo_path=".")
    store.materialize_incremental(end_date=datetime.now(tz=timezone.utc))


if __name__ == "__main__":
    materialize()
