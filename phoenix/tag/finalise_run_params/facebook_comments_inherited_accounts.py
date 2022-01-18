"""Finalise run parameters for facebook_comments accounts and objects with account classes."""
from typing import Any, Dict, Optional

import dataclasses

from phoenix.common.run_params import base, general


@dataclasses.dataclass
class FacebookCommentsInheritedAccountsFinaliseRunParamsURLs(base.RunParams):
    """Finalise accounts URLS."""

    config: Dict[str, Any]
    facebook_comments_final: str
    facebook_posts_objects_accounts_classes: str
    objects_accounts_classes_final: str


@dataclasses.dataclass
class FacebookCommentsInheritedAccountsFinaliseRunParams(base.RunParams):
    """Finalise accounts run params."""

    urls: FacebookCommentsInheritedAccountsFinaliseRunParamsURLs
    general: general.GeneralRunParams


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    year_filter: int,
    month_filter: int,
) -> FacebookCommentsInheritedAccountsFinaliseRunParams:
    """Create FacebookCommentsInheritedAccountsFinaliseRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)

    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": "facebook_comments",
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
    }

    urls = FacebookCommentsInheritedAccountsFinaliseRunParamsURLs(
        config=url_config,
        facebook_comments_final=art_url_reg.get_url(
            "tagging_runs-facebook_comments_final", url_config
        ),
        facebook_posts_objects_accounts_classes=art_url_reg.get_url(
            "final-objects_accounts_classes", url_config | {"OBJECT_TYPE": "facebook_posts"}
        ),
        objects_accounts_classes_final=art_url_reg.get_url(
            "final-objects_accounts_classes",
            url_config | {"OBJECT_TYPE": "facebook_comments_inherited"},
        ),
    )

    return FacebookCommentsInheritedAccountsFinaliseRunParams(
        general=general_run_params,
        urls=urls,
    )
