# YouTube Videos

| Column                    | dtype               | Description |
|---------------------------|---------------------|-------------|
| id                        | object              | ID |
| created_at                | datetime64[ns, UTC] | UTC timestamp that the video was first posted at |
| channel_id                | object              | Channel ID of the channel which posted the video |
| channel_title             | object              | Title (text) of the channel |
| title                     | object              | Title (text) of the video |
| description               | object              | Description (text) of the video |
| text                      | object              | Text of video:combined [title description] |
| video_url                 | object              | URL of video |
| channel_url               | object              | URL of video's channel |
| etag                      | object              | etag of the youtube#searchResult resource returned from the API |
| response_etag             | object              | etag of the youtube#searchListResponse resource returned from the API |
| timestamp_filter          | datetime64[ns, UTC] | Normalised column for filtering by timestamp. The UTC timestamp of the creation of the video |
| date_filter               | object (date32[day] in parquet) | Normalised column for filtering by date. The UTC date on the creation of the video |
| year_filter               | int64               | Normalised column for filtering by year. The year of the creation of the video |
| month_filter              | int64               | Normalised column for filtering by month. The month of the creation of the video |
| day_filter                | int64               | Normalised column for filtering by day. The day of the creation of the video |
| file_timestamp            | datetime64[ns, UTC] | The timestamp of the source file for this data |
