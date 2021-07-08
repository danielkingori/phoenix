"""Testing aws_comprehend module."""
import mock
import pandas as pd

from phoenix.tag.third_party_models import aws_comprehend


@mock.patch("phoenix.tag.third_party_models.aws_comprehend.comprehend_call")
def test_sentiment_analysis(mock_comprehend_call):
    """Test correct execute."""
    #  mock_comprehend_call.return_value()], 'ErrorList':None})
    objects_df = pd.DataFrame({})
    result = aws_comprehend.sentiment_analysis(objects_df, mock.Mock())
    pd.testing.assert_frame_equal(result, pd.DataFrame({}))
    mock_comprehend_call.assert_not_called()
