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
def test_update_labelling_sheets(m_papermill_execute, tenants_template_url_mock):
    runner = CliRunner()
    result = runner.invoke(
        cli.main_group.main_group,
        [
            "labelling",
            "update_labelling_sheets",
            "production",
            "tenant_id_1",
            "2022",
            "1",
            "facebook_posts",
            "tweets",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    default_parameters = {
        "RUN_DATETIME": "20000101T010101.000000Z",
        "RUN_DATE": "2000-01-01",
        "TENANT_ID": "tenant_id_1",
        "ARTIFACTS_ENVIRONMENT_KEY": "production",
    }
    output_base = "s3://data-lake/tenant_id_1/process/labelling_run/20000101T010101.000000Z/"
    calls = [
        mock.call(
            PathEndsWith("tag/labelling/push_account_labelling_sheet.ipynb"),
            f"{output_base}facebook_posts-push_account_labelling_sheet.ipynb",
            {
                **default_parameters,
                **{"GOAL_NUM_ROWS": 10000, "YEAR_FILTER": 2022, "MONTH_FILTER": 1},
                **{"OBJECT_TYPE": "facebook_posts"},
            },
        ),
        mock.call(
            PathEndsWith("tag/labelling/push_object_labelling_sheet.ipynb"),
            f"{output_base}facebook_posts-push_object_labelling_sheet.ipynb",
            {
                **default_parameters,
                **{"GOAL_NUM_ROWS": 10000, "YEAR_FILTER": 2022, "MONTH_FILTER": 1},
                **{"OBJECT_TYPE": "facebook_posts"},
            },
        ),
        mock.call(
            PathEndsWith("tag/labelling/push_account_labelling_sheet.ipynb"),
            f"{output_base}tweets-push_account_labelling_sheet.ipynb",
            {
                **default_parameters,
                **{"GOAL_NUM_ROWS": 10000, "YEAR_FILTER": 2022, "MONTH_FILTER": 1},
                **{"OBJECT_TYPE": "tweets"},
            },
        ),
        mock.call(
            PathEndsWith("tag/labelling/push_object_labelling_sheet.ipynb"),
            f"{output_base}tweets-push_object_labelling_sheet.ipynb",
            {
                **default_parameters,
                **{"GOAL_NUM_ROWS": 10000, "YEAR_FILTER": 2022, "MONTH_FILTER": 1},
                **{"OBJECT_TYPE": "tweets"},
            },
        ),
    ]

    m_papermill_execute.assert_has_calls(calls)
