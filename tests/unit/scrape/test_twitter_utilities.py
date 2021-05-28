"""Tests for twitter utilities."""

from phoenix.common import utils
from phoenix.scrape import twitter_utilities


def test_load_queries_from_csv():
    """Test loading queries as a list from a csv."""
    # Load path.
    file = "mock_queries.csv"
    filepath = utils.relative_path(f"./{file}", __file__)
    # Load queries.
    queries = twitter_utilities.load_queries_from_csv(filepath)
    # Test behavior.
    assert queries == ["query1", "query2", "query3"]
