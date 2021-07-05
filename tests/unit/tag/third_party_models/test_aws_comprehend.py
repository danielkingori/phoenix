"""Testing aws_comprehend module."""
import mock

from phoenix.tag.third_party_models import aws_comprehend

@mock.patch("phoenix.tag.third_party_models.aws_comprehend._make_api_call")
def test_excute(m_make_api_call):
    """Test correct execute."""
    objects_df = pd.DataFrame({})
    result =  aws_comprehend.execute(objects_df)
    pd.testing.assert_frame_equals(
        result,
        pd.DataFrame({})
    )

    m_make_api_call.assert_called_once_with(something)
