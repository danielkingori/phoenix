"""Test transformer."""
import json

from phoenix.common import utils
from phoenix.scrape.fb_post_parser import transform_parsed_comments_to_posts


def test_transform():
    """Get the test_get_files_url."""
    input_path = utils.relative_path("./input.json", __file__)
    output_path = utils.relative_path("./output.json", __file__)
    with open(input_path, "r") as f:
        _input = json.load(f)
    with open(output_path, "r") as f:
        output = json.load(f)
    transformed = transform_parsed_comments_to_posts.transform(_input)
    a, b = json.dumps(output, sort_keys=True), json.dumps(transformed, sort_keys=True)
    assert a == b
