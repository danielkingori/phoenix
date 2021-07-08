"""AWS comprehend functionality."""
import os
import sys

import boto3
import pandas as pd


AWS_ACCESS_KEY_ID = os.getenv("w")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def sentiment_analysis(objects: pd.DataFrame, client=None) -> pd.DataFrame:
    """Execute sentiment analysis via AWS comprehend models on the objects dataframe.

    Arguments:
        objects: objects dataframe. See docs/schemas/objects.md

    Returns:
        dataframe with sentiment and sentiment score columns added (careful, different row order).
    """
    if objects.empty:
        return pd.DataFrame({})

    objects = objects.copy()
    # TODO: restriction is 5000 bytes per text; how to optimally ensure this?
    objects["clean_text"] = objects["clean_text"].apply(lambda x: x[:1000])
    objects["sentiment"] = pd.NA
    objects["sentiment_scores"] = pd.NA
    objects["bytes_in_text"] = objects["clean_text"].apply(lambda s: sys.getsizeof(s))

    docs_per_batch = 25  # aka batch size (max=25)
    if client is None:
        client = boto3.client("comprehend")

    all_dfs = [objects[objects["language"] == "ar_izi"]]
    for lang in ["ar", "en"]:
        df = objects[objects["language"] == lang]
        for i in range(0, len(df), docs_per_batch):
            comprehend_response = comprehend_call(
                client,
                text_list=list(df["clean_text"].iloc[i : i + docs_per_batch]),
                language_code=lang,
            )
            print(comprehend_response)
            response_df = pd.DataFrame(comprehend_response["ResultList"])
            df["sentiment"].iloc[i : i + docs_per_batch] = response_df["Sentiment"].values
            df["sentiment_scores"].iloc[i : i + docs_per_batch] = response_df[
                "SentimentScore"
            ].values
            all_dfs.append(df)

    results_df = pd.concat(all_dfs)

    return results_df


def comprehend_call(client, text_list, language_code):
    """Function that executes the batch request to AWS comprehend.

    Args:
        client: boto3 client
        text_list (list): A list containing the text of the input documents.
        language_code (list): The language of the input documents.

    Returns:
        A list containing the text of the input documents.
        See https://docs.aws.amazon.com/comprehend/latest/dg/API_BatchDetectSentiment.html
    """
    return client.batch_detect_sentiment(
        TextList=text_list,
        LanguageCode=language_code,
    )
