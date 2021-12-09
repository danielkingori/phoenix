"""Module containing all functionality related to modelling and higher order inference.

This module is structured such that each next level down module must be named as a specific
inference task. E.g. "text_snippet_classification", or "sentiment_detection".

Within each inference task module, there must be a `validation.py` module which sets up `kotsu`
validation specs so that all competing models for the inference task are efficiently and fairly
compared.

It is up to each inference task to specify the interface that models for that task should
implement, but it is strongly recommended that all inference tasks specify that models should
implement APIs conformant to scikit-learn: https://scikit-learn.org/stable/developers/develop.html
Inference tasks should not deviate from this API without very good reason.
"""
