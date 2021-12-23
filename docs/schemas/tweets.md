# Tweets dataframe

Final tweets dataframe.

Data is mapped from `objects`, `topics` and data from twitter API: https://developer.twitter.com/en/docs/labs/tweets-and-users/api-reference/get-tweets-id.

| Column                    | dtype               | Description |
|---------------------------|---------------------|-------------|
| id_str                    | int64               | ID |
| created_at                | datetime64[ns, UTC] | UTC timestamp that the tweet was created |
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
| timestamp_filter          | datetime64[ns, UTC] | Normalised column for filtering by timestamp. UTC timestamp that the tweet was created |
| date_filter               | object (date32[day] in parquet) | Normalised column for filtering by date. UTC timestamp that the tweet was created |
| year_filter               | int64               | Normalised column for filtering by year. Year that the tweet was created |
| month_filter              | int64               | Normalised column for filtering by month. Month that the tweet was created |
| day_filter                | int64               | Normalised column for filtering by day. Day of the month that the tweet was created |
| user_id                   | int64               | User id that made the tweet |
| user_id_str               | object              | User id string that made the tweet |
| user_name                 | object              | User name that made the tweet |
| user_screen_name          | object              | User screen name that made the tweet |
| user_location             | object              | User location that made the tweet |
| user_description          | object              | User description that made the tweet |
| user_url                  | object              | User Url that made the tweet |
| user_protected            | bool                | If the user that made the tweet is protected |
| user_created_at           | datetime64[ns, UTC] | When the user that made the tweet was created  |
| user_geo_enabled          | bool                | If the user that made the tweet has geo enabled |
| user_verified             | bool                | If the user that made the tweet is verified |
| user_lang                 | object              | The configured language of the user that made the tweet |

## Additional columns with the tensions inference
see (`objects_tensions`)[docs/schemas/objects.md#Objects_tensions]

# Tweets topics

This has the same data as tweets dataframe but has the columns as below.

There can be multiple topics for a tweets in this case the tweets data is repeated.

| Column                  | dtype          | Description |
|-------------------------|----------------|-------------|
| topic                   | object         | topic that phoenix found in the text |
