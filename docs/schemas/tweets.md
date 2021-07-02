# Tweets dataframe

Final tweets dataframe.

| Column                    | dtype               | Description |
|---------------------------|---------------------|-------------|
| id_str                    | int64               | ID |
| created_at                | datetime64[ns, UTC] | UTC timestame that the tweet was created |
| id                        | int64               | ID |
| full_text                 | object              | Text of tweets |
| truncated                 | bool                | If the text has been truncated |
| display_text_range        | object              | - |
| metadata                  | object              | - |
| source                    | object              | - |
| in_reply_to_status_id     | float64             | - |
| in_reply_to_status_id_str | float64             | - |
| in_reply_to_user_id       | float64             | - |
| in_reply_to_user_id_str   | float64             | - |
| in_reply_to_screen_name   | object              | Currently None |
| geo                       | float64             | Currently None |
| coordinates               | float64             | Currently None |
| contributors              | float64             | Currently None |
| is_quote_status           | bool                | Is a quote of another tweet |
| retweet_count             | int64               | The number of times this tweet was retweeted |
| favorite_count            | int64               | The number of time this tweet was favorited |
| favorited                 | bool                | If the tweet is a favorited of another tweet |
| retweeted                 | bool                | If the tweet is an official retweet |
| lang                      | object              | Language that twitter detected |
| possibly_sensitive        | float64             | Chance that the tweet contains sensitive text |
| quoted_status_id          | float64             | - |
| quoted_status_id_str      | float64             | - |
| quoted_status_permalink   | object              | - |
| withheld_in_countries     | object              | - |
| text                      | object              | The text that the tagging pipeline used  |
| object_type               | object              | tweet |
| language_from_api         | object              | repeat of lang |
| clean_text                | object              | The cleaned version of the text |
| language                  | object              | The language that phoenix detected |
| confidence                | float64             | Confidence of the phoenix language detection |
| is_unofficial_retweet     | bool                | Does phoenix think it is an unofficial tweet |
| is_retweet                | bool                | Does phoenix think it is a retweet, both official and unofficial |
| is_key_object             | bool                | Does phoenix think the tweet is relevant |
| features                  | object              | List of features of the text that phoenix calculated |
| features_count            | object              | List of counts of the features that phoenix calculated |
