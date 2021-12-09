"""Supervise learning inference task of classifying text snippets into varied multi-classes.

Text snippets are the general term for any small string of text scraped from a social media
platform.  Examples include: facebook posts, facebook comments, twitter posts, youtube video
descriptions, youtube comments.

Target classes are generally specific and unique to a certain application of Phoenix to researchers
or a research question. Input data is usually created by scraping social media platforms, and then
researchers manually labelling a relatively small set of text snippets.

This is a _multilabel classification_ task as per:
https://scikit-learn.org/stable/modules/multiclass.html#multilabel-classification

Training data should be of format:
    - X: df with columns: `text`, and optionally any extra meta data about the snippet that is
      available at scrape time
    - y: df/matrix one-hot (0s and 1s only) of shape [n_samples, n_classes], 1 denoting sample (the
      text snippet) has been labelled as corresponding class
    - y_meta: optional extra data gained from manual labellers, can be concat (axis=1) to y, likely
      "features" which are strings that are assumed to denote strong signal as to corresponding
      class
"""
