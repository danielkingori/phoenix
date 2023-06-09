# Facebook comments

Final facebook comments dataframe

| Column                  | dtype          | Description |
|-------------------------|----------------|-------------|
| id                      | int64          | Comment id |
| post_id                 | int64          | Post id that was taken from the url that was scraped |
| file_id                 | object         | File that was saved at scrape time |
| parent_id               | int64          | The parent comment or post id |
| post_created            | datetime64[ns] | Timestamp of the create of the comment |
| timestamp_filter        | datetime64[ns, UTC] | Normalised column for filtering by timestamp. The UTC timestamp of the creation of the comment |
| date_filter             | object (date32[day] in parquet) | Normalised column for filtering by date. The UTC date on the creation of the comment |
| year_filter             | int64          | Normalised column for filtering by year. The year of the creation of the comment |
| month_filter            | int64          | Normalised column for filtering by month. The month of the creation of the comment |
| day_filter              | int64          | Normalised column for filtering by day. The day of the creation of the comment |
| text                    | object         | Text of the comment |
| reactions               | int64          | Number of reactions |
| top_sentiment_reactions | object         | rough type of reactions reported in order from most to third-most. eg [Like, Haha, Love] |
| user_display_name       | object         | Displayed user name of the commenter |
| username                | object         | Facebook id or username of the commenter |
| position                | object         | "post", "comment", or "subcomment" |

## Additional columns with the tensions inference
see (`objects_topics`)[docs/schemas/objects.md#Objects_tensions]

# Facebook comments topics

This has the same data as facebook comments dataframe but has the columns as below.

There can be multiple topics for a facebook comment in this case the facebook comment data is repeated.

| Column                  | dtype          | Description |
|-------------------------|----------------|-------------|
| topic                   | object         | topic that phoenix found in the text |
