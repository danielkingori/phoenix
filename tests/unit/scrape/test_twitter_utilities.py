"""Tests for twitter utilities."""

import pathlib

from phoenix.scrape import twitter_utilities


def test_load_queries_from_csv():
    """Test loading queries as a list from a csv."""
    # Load path.
    file = "mock_queries.csv"
    filepath = "file:" + str(pathlib.Path(__file__).parents[0] / file)
    # Load queries.
    queries = twitter_utilities.load_queries_from_csv(filepath)
    # Test behavior.
    assert queries == ["query1", "query2", "query3"]
