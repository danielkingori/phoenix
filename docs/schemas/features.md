# Features dataframe

The Features dataframe has a row for every feature in the objects dataframe.

This means that the dtypes are the same apart from the `features` and `features_count`. Also that data is repeated for each object over it's features.

| Column                | dtype   | Description |
|-----------------------|---------|-------------|
| object_id             | object  | |
| text                  | object  | |
| object_type           | object  | |
| language_from_api     | object  | |
| retweeted             | bool    | |
| clean_text            | object  | |
| language              | object  | |
| confidence            | float64 | |
| is_unofficial_retweet | bool    | |
| is_retweet            | bool    | |
| features              | object  | The feature |
| features_count        | int64   | The number of times the feature appears in the object |
| is_key_feature        | bool    | is a relevant feature |
