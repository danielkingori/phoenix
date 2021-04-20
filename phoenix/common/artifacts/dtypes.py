"""Data types for the artifacts package."""
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ArtifactDataFrame:
    """ArtifactDataFrame object.

    Object is an immutable dataclass object, used to represent
    an DataFrame that has been persisted as an artifact.
    """

    url: str
    dataframe: pd.DataFrame
