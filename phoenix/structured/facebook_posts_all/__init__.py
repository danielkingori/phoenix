"""Init."""
import prefect

from phoenix.structured.common import urls
from phoenix.structured.facebook_posts_all import load, transform


with prefect.Flow("structured/facebook_posts_all") as flow:
    input_dir = prefect.Parameter("input_dir")
    output_dir = prefect.Parameter("output_dir")
    file_urls = urls.get_list_of_urls_in_directory(input_dir)
    # Functions for tasks have a more descriptive name to help with debugging
    facebook_posts_all_per_file = transform.transform_task.map(file_urls)
    load.load_task.map(
        df=facebook_posts_all_per_file, facebook_posts_all_url=prefect.unmapped(output_dir)
    )
