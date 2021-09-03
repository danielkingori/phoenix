"""Finalise facebook posts, facebook comments and tweets.

This will join objects and topic data frames to respective data source dataframe.
"""

LANGUAGE_SENTIMENT_COLUMNS = [
    "language_sentiment",
    "language_sentiment_score_mixed",
    "language_sentiment_score_neutral",
    "language_sentiment_score_negative",
    "language_sentiment_score_positive",
]


def join_objects_to_facebook_posts(objects, language_sentiment_objects, facebook_posts):
    """Join the objects to the facebook_posts."""
    objects = objects.set_index("object_id")
    language_sentiment_objects = language_sentiment_objects.set_index("object_id")
    objects = objects.join(language_sentiment_objects[LANGUAGE_SENTIMENT_COLUMNS])
    facebook_posts["object_id"] = facebook_posts["phoenix_post_id"].astype(str)
    facebook_posts = facebook_posts.set_index("object_id")
    return facebook_posts.join(objects, rsuffix="_objects")


def join_objects_to_facebook_comments(objects, language_sentiment_objects, facebook_comments):
    """Join the objects to the facebook_comments."""
    objects = objects.set_index("object_id")
    language_sentiment_objects = language_sentiment_objects.set_index("object_id")
    objects = objects.join(language_sentiment_objects[LANGUAGE_SENTIMENT_COLUMNS])
    facebook_comments["object_id"] = facebook_comments["id"].astype(str)
    facebook_comments = facebook_comments.set_index("object_id")
    return facebook_comments.join(objects, rsuffix="_objects")


def join_objects_to_tweets(objects, language_sentiment_objects, tweets):
    """Join the objects to the tweets."""
    objects = objects.set_index("object_id")
    objects = objects.drop(columns=["retweeted", "text", "language_from_api"])
    language_sentiment_objects = language_sentiment_objects.set_index("object_id")
    objects = objects.join(language_sentiment_objects[LANGUAGE_SENTIMENT_COLUMNS])
    tweets["object_id"] = tweets["id_str"].astype(str)
    tweets = tweets.set_index("object_id")
    return tweets.join(objects)


def join_topics_to_facebook_posts(topics, facebook_posts):
    """Join the topics to the facebook_posts."""
    facebook_posts_df = facebook_posts.copy()
    facebook_posts_df["object_id"] = facebook_posts_df["phoenix_post_id"].astype(str)
    facebook_posts_df = facebook_posts_df.set_index("object_id")
    topics_df = topics[["object_id", "topic", "matched_features"]]
    topics_df = topics_df.set_index("object_id")
    result_df = topics_df.join(facebook_posts_df, how="right")
    return result_df.reset_index()


def join_topics_to_tweets(topics, tweets):
    """Join the topics to the tweets."""
    tweets_df = tweets.copy()
    tweets_df["object_id"] = tweets_df["id_str"].astype(str)
    tweets_df = tweets_df.set_index("object_id")
    tweets_df = tweets_df.drop(columns=["retweeted"])
    topics_df = topics[["object_id", "topic", "matched_features"]]
    topics_df = topics_df.set_index("object_id")
    result_df = topics_df.join(tweets_df, how="right")
    return result_df.reset_index()


def join_topics_to_facebook_comments(topics, facebook_comments):
    """Join the topics to the tweets."""
    facebook_comments_df = facebook_comments.copy()
    facebook_comments_df["object_id"] = facebook_comments_df["id"].astype(str)
    facebook_comments_df = facebook_comments_df.set_index("object_id")
    topics_df = topics[["object_id", "topic", "matched_features"]]
    topics_df = topics_df.set_index("object_id")
    result_df = topics_df.join(facebook_comments_df, how="right")
    return result_df.reset_index()
