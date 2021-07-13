# Facebook comments

Final facebook comments dataframe

| Column                  | dtype          | Description |
|-------------------------|----------------|-------------|
| id                      | int64          | Comment id |
| post_id                 | int64          | Post id that was taken from the url that was scraped |
| file_id                 | object         | File that was saved at scrape time |
| parent_id               | int64          | The parent comment or post id |
| post_created            | datetime64[ns] | Timestamp of the create of the comment |
| text                    | object         | Text of the comment |
| reactions               | int64          | Number of reactions |
| top_sentiment_reactions | object         | rough type of reactions reported in order from most to third-most. eg [Like, Haha, Love] |
| user_display_name       | object         | Displayed user name of the commenter |
| username                | object         | Facebook id or username of the commenter |
| position                | object         | "post", "comment", or "subcomment" |
| text_object             | object         | The text that the tagging pipeline used |
| object_type             | object         | facebook_comment |
| language_from_api       | object         | - |
| clean_text              | object         | The cleaned version of the text |
| language                | object         | Phoenix detected language |
| confidence              | float64        | Confidence of the phoenix language detection |
| is_unofficial_retweet   | bool           | - |
| is_retweet              | bool           | - |
| is_key_object           | bool           | Does phoenix think the tweet is relevant |
| features                | object         | List of features of the text that phoenix calculated |
| features_count          | object         | List of counts of the features that phoenix calculated |


# Facebook comments topics

This has the same data as facebook comments dataframe but has the columns as below.

There can be multiple topics for a facebook comment in this case the facebook comment data is repeated.

| Column                  | dtype          | Description |
|-------------------------|----------------|-------------|
| topic                   | object         | topic that phoenix found in the text |
