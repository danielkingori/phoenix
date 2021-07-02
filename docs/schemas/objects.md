# Objects dataframe

The objects is the dataframe that will be send down the tagging pipeline.
This creates one schema for all sources that the tagging pipeline will process and complete.

| Column                | dtype   | Description |
|-----------------------|---------| ------------|
| object_id             | object  | The unique object id |
| text                  | object  | Text to analyse |
| object_type           | object  | Enum based on `phoenix/tag/data_pull/constants.py` |
| language_from_api     | object  | Language key that came from the scraped data. Thus not computed by phoenix. |
| retweeted             | bool    | Retweeted bool from tweets source data |
| clean_text            | object  | The text after cleaning |
| language              | object  | Language computed by phoenix |
| confidence            | float64 | Confidence of the language |
| is_unofficial_retweet | bool    | phoenix thinks it is an unofficial retweet |
| is_retweet            | bool    | phoenix thinks it is a retweet |
| is_key_object         | bool    | phoenix thinks it is relevent data |
| features              | object  | a list of string that are the features computed for the text |
| features_count        | object  | a list of integers that are the counts of the features at the same index in the features column |
