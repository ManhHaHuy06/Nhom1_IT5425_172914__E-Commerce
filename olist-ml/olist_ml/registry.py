"""Model artifacts: what is stored in MinIO and the version check that protects against unpickling with another scikit-learn."""
from __future__ import annotations

import platform

ARTIFACT_FILES = ("late_risk.joblib", "forecast_bands.json", "thresholds.json", "metrics.json", "model_card.json")


def runtime_versions() -> dict:
    import numpy
    import pandas
    import sklearn
    return {"python": platform.python_version(), "scikit-learn": sklearn.__version__, "numpy": numpy.__version__, "pandas": pandas.__version__}


def _minor(v: str) -> str:
    return ".".join(v.split(".")[:2])


def version_mismatches(card: dict, runtime: dict | None = None) -> list:
    """Empty list = OK. A .joblib is a pickle: it must be read with the same scikit-learn / numpy minor version it was written with."""
    runtime, trained = runtime or runtime_versions(), card.get("versions", {})
    return [f"{k}: model trained with {trained[k]}, running with {runtime[k]}" for k in ("scikit-learn", "numpy")
            if k in trained and _minor(trained[k]) != _minor(runtime[k])]
