import logging
import os
from contextlib import asynccontextmanager

import mlflow.pyfunc
import numpy as np
from fastapi import FastAPI
from feast import FeatureStore

from ml.train import FEATURE_COLS, TasteProfileModel

logger = logging.getLogger(__name__)

FEATURES_REPO = os.path.join(os.path.dirname(__file__), "..", "features")

# module-level singletons populated during lifespan startup
feast_store: FeatureStore | None = None
ml_model: mlflow.pyfunc.PyFuncModel | None = None
model_version: str = "unknown"
taste_vector: np.ndarray | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global feast_store, ml_model, model_version, taste_vector

    logger.info("Loading Feast feature store...")
    feast_store = FeatureStore(repo_path=FEATURES_REPO)

    logger.info("Loading Production model from MLflow...")
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    ml_model = mlflow.pyfunc.load_model("models:/spotify-taste-recommender/Production")

    # extract taste vector from the underlying python model for scoring in spotify endpoint
    python_model: TasteProfileModel = ml_model._model_impl.python_model
    taste_vector = python_model.taste_vector

    client = mlflow.MlflowClient()
    versions = client.get_latest_versions("spotify-taste-recommender", stages=["Production"])
    model_version = versions[0].version if versions else "unknown"

    logger.info(f"Model version {model_version} loaded")
    yield

    logger.info("Shutting down")


def get_store() -> FeatureStore:
    return feast_store


def get_model() -> mlflow.pyfunc.PyFuncModel:
    return ml_model


def get_model_version() -> str:
    return model_version


def get_taste_vector() -> np.ndarray:
    return taste_vector
