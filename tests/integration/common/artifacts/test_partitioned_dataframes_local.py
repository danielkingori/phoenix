"""Artefacts DataFrame functionality test on local file system."""
import pandas as pd
import pytest

from phoenix.common import artifacts


def test_dataframe_artifact_partitions(tmp_path):
    """Test full functionality of DataFrame artifacts."""
    test_artefact_dir = tmp_path / "dataframe_artifacts/"
    artefact_basename = "df"
    artefact_url = f"{test_artefact_dir}{artefact_basename}"
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "group": [1, 1, 2, 2],
        }
    )
    df["group"] = df["group"].astype("category")

    a_df = artifacts.partitioned_dataframes.persist(
        artefact_url, df, {"partition_cols": ["group"]}
    )
    e_url = f"{test_artefact_dir}{artefact_basename}"
    assert a_df.url == e_url
    pd.testing.assert_frame_equal(a_df.dataframe, df)

    a_df_2 = artifacts.partitioned_dataframes.get(artefact_url)
    assert a_df.url == a_df_2.url
    pd.testing.assert_frame_equal(a_df.dataframe, a_df_2.dataframe)

    artifacts.partitioned_dataframes.delete(a_df_2)

    with pytest.raises(FileNotFoundError):
        artifacts.partitioned_dataframes.get(a_df_2.url)
