"""RecalculateSFLMRunParams tests."""
import pytest

from phoenix.tag.labelling.run_params import dtypes, recalculate_sflm


ARTIFACTS_ENVIRONMENT_KEY = "production"


@pytest.mark.parametrize(
    (
        "object_type, expected_worksheet_name"
        ",tenant_id, expected_spreadsheet_name"
        ",expected_tenant_folder_id"
    ),
    [
        (
            "youtube_videos",
            "youtube_videos_feature_mappings",
            "tenant_id_1",
            "tenant_id_1_class_mappings",
            "folder_id_1",
        ),
        (
            "facebook_posts",
            "facebook_posts_feature_mappings",
            "tenant_id_1",
            "tenant_id_1_class_mappings",
            "folder_id_1",
        ),
        (
            "youtube_videos",
            "youtube_videos_feature_mappings",
            "tenant_id_3",
            "tenant_id_3_class_mappings",
            "folder_id_3",
        ),
        (
            "tweets",
            "tweets_feature_mappings",
            "tenant_id_3",
            "tenant_id_3_class_mappings",
            "folder_id_3",
        ),
    ],
)
def test_create(
    object_type,
    expected_worksheet_name,
    tenant_id,
    expected_spreadsheet_name,
    expected_tenant_folder_id,
    tenants_template_url_mock,
):
    """Test create of the youtube_videos run params."""
    run_params = recalculate_sflm.create(
        artifacts_environment_key=ARTIFACTS_ENVIRONMENT_KEY,
        tenant_id=tenant_id,
        run_datetime_str=None,
        object_type=object_type,
    )

    assert run_params
    assert isinstance(run_params, dtypes.RecalculateSFLMRunParams)
    assert run_params.spreadsheet_name == expected_spreadsheet_name
    assert run_params.worksheet_name == expected_worksheet_name
    assert run_params.tenant_folder_id == expected_tenant_folder_id

    urls = run_params.urls
    assert isinstance(urls, dtypes.RecalculateSFLMRunParamsURLs)
    assert urls.config["OBJECT_TYPE"] == object_type
