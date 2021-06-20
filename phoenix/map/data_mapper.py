"""data_mapper.py."""
import logging
import hashlib
import json
import pandas as pd
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from phoenix.common.artifacts import json as json_arti
from phoenix.map.config import config


# Logging config
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('DataMapper')
logging.getLogger('DataMapper').setLevel(logging.DEBUG)


class DataMapper:
    """
    A class used to map raw post data to normalised json
    files ready to be loaded to a postgres db.

    Attributes
    ----
    data_origin: str
        a code representing the social platform
        the raw data was sourced from E.G. fb | tw
    expected_output_count: int
        an integer representing the number of expected
        output files to be created based on the number
        of mapping configs for each social platform source
    files_to_process: List[str]
        a list of files found in the source incoming dir
    output_files: List[str]
        a list of files created by the mapper

    Methods
    ----
    get_files_to_process()
        Gets a list of files to process from the source
        incoming dir
    map_data_to_idl_files()
        Maps the raw post data to normalised json files
        ready to be loaded to the postgres db
    archive_processed_files()
        Moves processed raw post files to an archive dir
        so they dont get processed more than once
    """
    SOURCE_FILE_SUFFIX = '.json'

    def __init__(self, data_origin: str):
        self.data_origin = data_origin
        self._env_config = config.get_env_config()
        self._map_config = config.get_map_config(data_origin=self.data_origin)
        self._batch_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._data_dir_base = self._env_config['data_dir_base']
        self._data_dir_idl = self._env_config['data_dir_idl']
        self._data_dir_arch = self._env_config['data_dir_arch']
        self.expected_output_count = len(self._map_config)
        self.files_to_process = []
        self.output_files = []
        self._make_required_dirs([
            self._data_dir_base,
            self._data_dir_idl,
            self._data_dir_arch
        ])

    @staticmethod
    def _normalize_nested_json(df: pd.DataFrame, cfg: Dict[str, Optional[Any]]) -> pd.DataFrame:
        logger.info(f"Starting json normalization process for {cfg['name']}.")
        json_obj = [
            {
                # Add source file name to help calc curr in pg db
                'src_file_name': row['src_file_name'],
                # Hash pk to use as pk|fk in pg db
                cfg['foreign_key_field'] + '_hash': hashlib.md5(
                    json.dumps(row[cfg['foreign_key_field']]).encode()
                ).hexdigest(),
                # Add field containing array that needs normalising
                cfg['normalize_field']: row[cfg['normalize_field']]
            }
            for i, row in df.iterrows()
        ]
        # Drop dupes is mainly for dim datasets
        df = pd.json_normalize(json_obj).drop_duplicates()
        return df

    @staticmethod
    def _reshape_df(df, map_cfg) -> pd.DataFrame:
        logger.info('Reshaping dataframe to include full universe of columns.')
        # Create list of arrived cols
        arrived_columns = [col for col in df.columns]
        # Combine arrived cols with all expected cols
        all_columns = list(set(arrived_columns + map_cfg['columns']))
        # Create a template df with all required cols
        df_template = pd.DataFrame(columns=all_columns)
        # Reshape passed df to new df containing all expected cols
        reshaped_df = pd.concat(objs=[df_template, df], ignore_index=True, axis=0)
        return reshaped_df

    @staticmethod
    def _make_required_dirs(lists_of_dirs: List) -> None:
        logger.info('Creating required dirs if they don''t already exist.')
        for required_dir in lists_of_dirs:
            os.makedirs(os.path.join(*required_dir), exist_ok=True)

    def _load_files_to_df(self, files: List, map_cfg: Dict[str, Optional[Any]]) -> pd.DataFrame:
        logger.info(f"Loading base files to dataframes to process {map_cfg['name']}.")
        df_list = []
        for file in files:
            json_obj = json_arti.get(
                artifacts_json_url=os.path.join(*self._data_dir_base, file)
            ).obj
            artifact_df = pd.DataFrame(json_obj)
            artifact_df['src_file_name'] = file
            df_list.append(artifact_df)
        # Concat df's together if running map job for multiple files
        loaded_df = pd.concat(objs=df_list, ignore_index=True, axis=0)
        # Reshape df so that it is consisted shape to by copy'd to pg db
        reshaped_df = self._reshape_df(loaded_df, map_cfg)
        return reshaped_df

    def get_files_to_process(self) -> None:
        """
        Gets a list of files to process from the source incoming dir.
        return: None
        rtype: None
        """
        logger.info(f"""
        Searching base dir {os.path.join(*self._data_dir_base)} for files to process.
        """)
        # Get list of file names in base/to_process dir
        self.files_to_process = [
            obj for obj in os.listdir(os.path.join(*self._data_dir_base))
            if str(obj).endswith(self.SOURCE_FILE_SUFFIX)
        ]

    def map_data_to_idl_files(self) -> None:
        """
        Maps the raw post data to normalised json files ready to be
        loaded to the postgres db.
        return: None
        rtype: None
        """
        logger.info('Starting data map process.')
        # Loop for when multiple datasets are extracted from single source file
        for cfg in self._map_config:
            # Load data file(s) to single df
            posts_df = self._load_files_to_df(self.files_to_process, cfg)
            # Create new df of just cols we want
            df = posts_df[cfg['columns']].copy()
            # Check for and run processing step(s)
            if cfg['processing_steps']:
                for process_step in cfg['processing_steps']:
                    logger.info(f"Processing step {process_step} for {cfg['name']}")
                    df = eval(f"self.{process_step}(df, cfg)")
            df = df.fillna("")
            # Persist idl df to idl json ready for pg db ingest
            persisted_object = json_arti.persist(
                artifacts_json_url=os.path.join(
                    *self._data_dir_idl,
                    f"{cfg['name']}_{self._batch_ts}.json"
                ),
                obj=df.to_dict(orient='records')
            )
            self.output_files.append(persisted_object.url)

    def archive_processed_files(self) -> None:
        """
        Moves processed raw post files to an archive dir so they dont
        get processed more than once.
        return: None
        rtype: None
        """
        logger.info('Starting data archive process.')
        cwd_path = os.getcwd()
        for file in self.files_to_process:
            os.rename(
                src=os.path.join(cwd_path, *self._data_dir_base, file),
                dst=os.path.join(cwd_path, *self._data_dir_arch, file)
            )


def main() -> int:
    """Main program. Create and run data mapper."""
    # Instantiate data_mapper
    data_mapper = DataMapper(data_origin='fb')
    # Poll base dir for new files to process
    data_mapper.get_files_to_process()
    # If new files exist map base data to idl data
    if len(data_mapper.files_to_process) > 0:
        logger.info('New files to process found.')
        data_mapper.map_data_to_idl_files()
    else:
        logger.info('No new files to process found.')
        logger.info('Exiting data map process.')
    # If correct number of files created archive processed files
    if len(data_mapper.output_files) == data_mapper.expected_output_count:
        logger.info('Output file count is as expected.')
        data_mapper.archive_processed_files()
    else:
        logger.info('Output file count is not as expected.')
        logger.info('Exiting data archive process.')
    return 0


if __name__ == '__main__':
    main()
