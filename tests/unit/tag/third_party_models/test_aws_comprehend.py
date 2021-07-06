"""Testing aws_comprehend module."""
import mock
import pandas as pd

from phoenix.tag.third_party_models import aws_comprehend


api_return_mock = dict(
    ResultList=[
        {"Index": 0, "Sentiment": "POSITIVE", "SentimentScore": {}},
    ],
    ErrorList=[],
)


@mock.patch("phoenix.tag.third_party_models.aws_comprehend.comprehend_call")
def test_sentiment_analysis(mock_comprehend_call):
    """Test correct execute."""
    objects_df = pd.DataFrame({})
    result = aws_comprehend.sentiment_analysis(objects_df, mock.Mock())
    pd.testing.assert_frame_equal(result, pd.DataFrame({}))
    mock_comprehend_call.assert_not_called()

    objects_df = pd.DataFrame({"clean_text": ["text"], "language": ["en"]})
    mock_comprehend_call.return_value = api_return_mock
    result = aws_comprehend.sentiment_analysis(objects_df, mock.Mock())
    assert (
        len(
            set(result.columns)
            - set(["clean_text", "language", "sentiment", "sentiment_scores", "bytes_in_text"])
        )
        == 0
    )
