"""Finalise functionality."""


def join_objects_to_facebook_posts(objects, facebook_posts):
    """Join the objects to the facebook_posts."""
    objects = objects.set_index("object_id")
    facebook_posts["object_id"] = facebook_posts["phoenix_post_id"].astype(str)
    facebook_posts = facebook_posts.set_index("object_id")
    return facebook_posts.join(objects, rsuffix="_objects")


def join_objects_to_facebook_comments(objects, facebook_comments):
    """Join the objects to the facebook_comments."""
    objects = objects.set_index("object_id")
    facebook_comments["object_id"] = facebook_comments["id"].astype(str)
    facebook_comments = facebook_comments.set_index("object_id")
    return facebook_comments.join(objects, rsuffix="_objects")


def join_objects_to_tweets(objects, tweets):
    """Join the objects to the tweets."""
    objects = objects.set_index("object_id")
    objects = objects.drop(columns=["retweeted"])
    tweets["object_id"] = tweets["id_str"].astype(str)
    tweets = tweets.set_index("object_id")
    return tweets.join(objects)


def join_topics_to_facebook_posts(topics, facebook_posts):
    """Join the topics to the facebook_posts."""
    facebook_posts_df = facebook_posts.copy()
    facebook_posts_df["object_id"] = facebook_posts_df["phoenix_post_id"].astype(str)
    facebook_posts_df = facebook_posts_df.set_index("object_id")
    topics_df = topics.set_index("object_id")
    result_df = topics_df.join(facebook_posts_df, how="right")
    return result_df.reset_index()


def join_topics_to_tweets(topics, tweets):
    """Join the topics to the tweets."""
    tweets_df = tweets.copy()
    tweets_df["object_id"] = tweets_df["id_str"].astype(str)
    tweets_df = tweets_df.set_index("object_id")
    tweets_df = tweets_df.drop(columns=["retweeted"])
    topics_df = topics.set_index("object_id")
    result_df = topics_df.join(tweets_df, how="right")
    return result_df.reset_index()
