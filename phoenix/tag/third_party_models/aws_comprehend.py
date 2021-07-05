"""AWS comprehend functionality."""
import pandas as pd


def execute(objects: pd.DataFrame) -> pd.DataFrame:
    """Execute the aws comprehend models on the objects dataframe.

    Arguments:
        objects: objects dataframe. See docs/schemas/objects.md

    Returns:
        TODO
    """
    aribic_texts = objects_df["arabic"]
    sentiment_ar = _make_api_call(aribic_texts)
    dosomething here
    return pd.DataFrame({})



def _make_api_call(arg1) -> Dict:
    """Private function to call api."""
    # TODO make boto3 api call
    ...
