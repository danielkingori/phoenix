# YouTube Comments

| Column                    | dtype               | Description |
|---------------------------|---------------------|-------------|
| id                        | object              | ID |
| published_at              | datetime64[ns, UTC] | UTC timestamp that the comment was first posted at |
| updated_at                | datetime64[ns, UTC] | UTC timestamp that the comment was updated at |
| text                      | object              | Text of comment when displayed |
| text_original             | object              | Original text of comment, may be unavailable i.e. `None` |
| like_count                | int64               | Number of likes the comment has currently |
| is_top_level_comment      | bool                | Whether the comment is a top level comment (True) or a reply to a comment (False) |
| total_reply_count         | int64               | Number of replies the top level comment has currently. Is `None` for non-top level comments |
| parent_comment_id         | object              | ID of the parent comment. Is `None` for top level comments |
| author_channel_id         | object              | ID of the commenters channel, may be unavaiable i.e. `None` |
| author_display_name       | object              | Comment authors display name |
| channel_id                | object              | Channel ID of the video on which the comment was posted to |
| video_id                  | object              | Video ID of the video on which the comment was posted to |
| etag                      | object              | etag of the youtube#comment resource returned from the API |
| response_etag             | object              | etag of the youtube#commentThreadListResponse resource returned from the API |
| timestamp_filter          | datetime64[ns, UTC] | Normalised column for filtering by timestamp. The UTC timestamp of the creation of the comment |
| date_filter               | object (date32[day] in parquet) | Normalised column for filtering by date. The UTC date on the creation of the comment |
| year_filter               | int64               | Normalised column for filtering by year. The year of the creation of the comment |
| month_filter              | int64               | Normalised column for filtering by month. The month of the creation of the comment |
| day_filter                | int64               | Normalised column for filtering by day. The day of the creation of the comment |
| file_timestamp            | datetime64[ns, UTC] | The timestamp of the source file for this data |
