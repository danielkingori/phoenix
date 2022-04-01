"""Data types for the artifacts package."""
from typing import Any, Dict, List, Union

from dataclasses import dataclass

import pandas as pd
from dask import dataframe as dd


@dataclass(frozen=True)
class ArtifactDataFrame:
    """ArtifactDataFrame object.

    Object is an immutable dataclass object, used to represent
    an DataFrame that has been persisted as an artifact.
    """

    url: str
    dataframe: pd.DataFrame


@dataclass(frozen=True)
class ArtifactDaskDataFrame:
    """ArtifactDaskDataFrame object.

    Object is an immutable dataclass object, used to represent
    an Dask DataFrame that has been persisted as an artifact.
    """

    url: str
    dataframe: dd.DataFrame


@dataclass(frozen=True)
class ArtifactJson:
    """ArtifactJson object.

    Object is an immutable dataclass object, used to represent
    an Json that has been persisted as an artifact.
    """

    url: str
    obj: Union[List, Dict[Any, Any], None]
