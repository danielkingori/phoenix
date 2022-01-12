"""Run parameters for recalculating the SFLM."""
from typing import Optional

from phoenix.common.run_params import general
from phoenix.tag.labelling.run_params import dtypes


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
) -> dtypes.RecalculateSFLMRunParams:
    """Create the Finalise run params for youtube_videos."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)
    urls = _get_urls(object_type)
    spreadsheet_name = f"{tenant_id}_class_mappings"
    worksheet_name = f"{object_type}_feature_mappings"

    return dtypes.RecalculateSFLMRunParams(
        general=general_run_params,
        urls=urls,
        spreadsheet_name=spreadsheet_name,
        worksheet_name=worksheet_name,
        tenant_folder_id=general_run_params.tenant_config.google_drive_folder_id,
    )


def _get_urls(
    object_type: str,
) -> dtypes.RecalculateSFLMRunParamsURLs:
    """Get FinaliseRunParamsURLs for youtube_videos."""
    url_config = {
        "OBJECT_TYPE": object_type,
    }
    return dtypes.RecalculateSFLMRunParamsURLs(
        config=url_config,
    )
