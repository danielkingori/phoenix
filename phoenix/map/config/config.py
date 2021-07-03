"""config.py."""
from typing import Dict, List, Union

import os

import yaml


def get_map_config(data_origin: str) -> List[Dict[str, Union[str, List[str]]]]:
    """Gets list of config dicts for each dataset that needs mapping.

    param data_origin: a code representing the social platform
        the raw data was sourced from E.G. fb | tw
    type data_origin: str
    return: List of config dicts for each dataset to be extracted
    rtype: List[Dict[str, Union[str, List[str]]]]
    """
    # Get all job configs
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "map_config.yml")
    ) as config_file:
        all_cfg = yaml.load(config_file, Loader=yaml.FullLoader)
    # Filter for origin
    config = [cfg for key, cfg in all_cfg.items() if cfg["data_origin"] == data_origin]
    return config


def get_env_config() -> Dict[str, List[str]]:
    """Gets environment config for each env type [local_dev|docker_dev|docker_prod].

    return: Dict of environment config settings
    rtype: Dict[str, List[str]]
    """
    # Get configs and set constants
    if os.environ.get("IS_DOCKER_CONTAINER", False):
        env_type = "docker_dev"
    else:
        env_type = "local_dev"
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "env_config.yml")
    ) as env_config_file:
        all_env_cfg = yaml.load(env_config_file, Loader=yaml.FullLoader)
    env_config = all_env_cfg[env_type]
    return env_config
