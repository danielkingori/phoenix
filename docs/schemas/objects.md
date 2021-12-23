# Objects dataframe

The objects is the dataframe that will be send down the tagging pipeline.
This creates one schema for all sources that the tagging pipeline will process and complete.

| Column                | dtype   | Description |
|-----------------------|---------| ------------|
| object_id             | object  | The unique object id |
| object_url            | object  | URL of the object |
| object_user_url       | object  | URL of the object's user |
| object_user_name      | object  | Name of the object's user |
| text                  | object  | Text to analyse |
| object_type           | object  | Enum based on `phoenix/tag/data_pull/constants.py` |
| language_from_api     | object  | Language key that came from the scraped data. Thus not computed by phoenix. |
| retweeted             | bool    | Retweeted bool from tweets source data |
| clean_text            | object  | The text after cleaning |
| language              | object  | Language computed by phoenix |
| confidence            | float64 | Confidence of the language |
| is_unofficial_retweet | bool    | phoenix thinks it is an unofficial retweet |
| is_retweet            | bool    | phoenix thinks it is a retweet |
| features              | object  | a list of string that are the features computed for the text |
| features_count        | object  | a list of integers that are the counts of the features at the same index in the features column |

# Objects_topics
 The objects_topics has the same schema as objects with the additional columns:

| Column                  | dtype          | Description |
|-------------------------|----------------|-------------|
| ... same as objects ... | ... | ... |
| topics                  | object         | a list of strings that is the computed topics for the text |
| has_topics              | boolean        | if the object has computed topics. This is false if the default topic is the only topic for the object. |

# Object_tensions dataframe

The object_tensions is the dataframe is the output of the tensions model. It has the same schema as objects_topics
This creates one schema for all sources that the tagging pipeline including the tensions model will
 process and complete.

|                              Column |   dtype |                                                Description                                               |
|------------------------------------:|--------:|:--------------------------------------------------------------------------------------------------------:|
| ... same as objects_topics ... | ... | ... |
| is_economic_labour_tension          | bool    | phoenix thinks this object is about economic and labour tension                                          |
| is_sectarian_tension                | bool    | phoenix thinks this object is about sectarian tension                                                    |
| is_environmental_tension            | bool    | phoenix thinks this object is about environmental tension                                                |
| is_political_tension                | bool    | phoenix thinks this object is about political tension                                                    |
| is_service_related_tension          | bool    | phoenix thinks this object is about service related tension                                              |
| is_community_insecurity_tension     | bool    | phoenix thinks this object is about community insecurity tension                                         |
| is_geopolitics_tension              | bool    | phoenix thinks this object is about geopolitics tension                                                  |
| is_intercommunity_relations_tension | bool    | phoenix thinks this object is about intercommunity relation tension. This includes host-refugee tensions |
| has_tension                         | bool    | phoenix thinks this object has one or more tensions |

