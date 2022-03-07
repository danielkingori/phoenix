"""Finalise run parameters for accounts and text snippets (objects) with account classes."""
from typing import Any, Dict, Optional

import dataclasses

from phoenix.common.run_params import base, general


@dataclasses.dataclass
class AccountsFinaliseRunParamsURLs(base.RunParams):
    """Finalise accounts URLS."""

    config: Dict[str, Any]
    input_objects_dataset: str
    input_accounts_classes: str
    tagging_runs_accounts_final: str
    tagging_runs_objects_accounts_classes_final: str
    accounts_final: str
    objects_accounts_classes_final: str


@dataclasses.dataclass
class AccountsFinaliseRunParams(base.RunParams):
    """Finalise accounts run params."""

    urls: AccountsFinaliseRunParamsURLs
    general: general.GeneralRunParams


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
    year_filter: int,
    month_filter: int,
    accounts_final_url: Optional[str] = None,
    objects_accounts_classes_final_url: Optional[str] = None,
) -> AccountsFinaliseRunParams:
    """Create AccountsFinaliseRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)

    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }

    if accounts_final_url is None:
        accounts_final_url = art_url_reg.get_url("final-accounts", url_config)
    if objects_accounts_classes_final_url is None:
        objects_accounts_classes_final_url = art_url_reg.get_url(
            "final-objects_accounts_classes", url_config
        )

    urls = AccountsFinaliseRunParamsURLs(
        config=url_config,
        input_objects_dataset=art_url_reg.get_url(
            f"tagging_runs-{object_type}_pulled", url_config
        ),
        input_accounts_classes=art_url_reg.get_url("sflm-account-object_type", url_config),
        tagging_runs_accounts_final=art_url_reg.get_url("tagging_runs-accounts_final", url_config),
        tagging_runs_objects_accounts_classes_final=art_url_reg.get_url(
            "tagging_runs-objects_accounts_classes_final", url_config
        ),
        accounts_final=accounts_final_url,
        objects_accounts_classes_final=objects_accounts_classes_final_url,
    )

    return AccountsFinaliseRunParams(
        general=general_run_params,
        urls=urls,
    )
