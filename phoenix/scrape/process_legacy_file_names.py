"""Process the legacy source files to write with new run datetime."""
import logging

import pandas as pd
import tentaclio

from phoenix.common.artifacts import source_file_name_processing


def reprocess_legacy_file_names(url_to_folder: str, output_url: str) -> pd.DataFrame:
    """Reprocess the legacy file names.

    This will copy all the legacy file names to the output url with
    non legacy file name.
    """
    for entry in tentaclio.listdir(url_to_folder):
        logging.info(f"Processing file: {entry}")
        source_file_name = source_file_name_processing.get_source_file_name(entry)
        if not source_file_name:
            raise RuntimeError(
                (
                    f"Unable to process file name {entry}."
                    " You may need to add a timestamp to the file name."
                    " Check phoenix/common/run_datetime.py for format."
                )
            )

        if source_file_name.is_legacy:
            new_url = f"{output_url}{source_file_name.non_legacy_file_name()}"
            logging.info(f"New url: {new_url}")
            tentaclio.copy(entry, new_url)
