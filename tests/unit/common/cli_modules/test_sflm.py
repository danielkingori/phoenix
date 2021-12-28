"""Test of the sflm CLI."""
import os
import pathlib

import mock
from click.testing import CliRunner
from freezegun import freeze_time

from phoenix.common import cli
from phoenix.common.artifacts import registry_environment


URL_PREFIX = "s3://data-lake/"


class PathEndsWith(str):
    def __eq__(self, other):
        assert isinstance(other, pathlib.PosixPath)
        return self in str(other)


@freeze_time("2000-01-1 01:01:01", tz_offset=0)
@mock.patch.dict(os.environ, {registry_environment.PRODUCTION_ENV_VAR_KEY: URL_PREFIX})
@mock.patch("phoenix.common.cli_modules.utils.run_notebooks")
def test_create_from_labels(m_papermill_execute, tenants_template_url_mock):
    runner = CliRunner()
    result = runner.invoke(
        cli.main_group.main_group,
        ["sflm", "create_from_labels", "production", "tenant_id_1", "facebook_posts", "tweets"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    default_parameters = {
        "RUN_DATETIME": "20000101T010101.000000Z",
        "RUN_DATE": "2000-01-01",
        "TENANT_ID": "tenant_id_1",
        "ARTIFACTS_ENVIRONMENT_KEY": "production",
    }
    output_base = "s3://data-lake/tenant_id_1/process/sflm_run/20000101T010101.000000Z/"
    calls = [
        mock.call(
            PathEndsWith("tag/labelling/pull_objects_labelling_sheet.ipynb"),
            f"{output_base}facebook_posts-pull_objects_labelling_sheet.ipynb",
            {**default_parameters, **{"OBJECT_TYPE": "facebook_posts"}},
        ),
        mock.call(
            PathEndsWith("tag/labelling/persist_sflm_to_config.ipynb"),
            f"{output_base}facebook_posts-persist_sflm_to_config.ipynb",
            {**default_parameters, **{"OBJECT_TYPE": "facebook_posts"}},
        ),
        mock.call(
            PathEndsWith("tag/labelling/pull_objects_labelling_sheet.ipynb"),
            f"{output_base}tweets-pull_objects_labelling_sheet.ipynb",
            {**default_parameters, **{"OBJECT_TYPE": "tweets"}},
        ),
        mock.call(
            PathEndsWith("tag/labelling/persist_sflm_to_config.ipynb"),
            f"{output_base}tweets-persist_sflm_to_config.ipynb",
            {**default_parameters, **{"OBJECT_TYPE": "tweets"}},
        ),
        mock.call(
            PathEndsWith("tag/labelling/backport_sflm_config_to_sfm_config.ipynb"),
            f"{output_base}backport_sflm_config_to_sfm_config.ipynb",
            {**default_parameters, **{"OBJECT_TYPES": ["facebook_posts", "tweets"]}},
        ),
    ]

    m_papermill_execute.assert_has_calls(calls)
