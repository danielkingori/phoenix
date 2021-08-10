# Facebook posts dataframe
 
Final dataframe of facebook posts.

In general these relate to the data from crowdtangle API: https://github.com/CrowdTangle/API/wiki/Post

| Column name                            | dtype               | Description                                                                                                       | 
|----------------------------------------|---------------------| ------------------------------------------------------------------------------------------------------------------|
| phoenix_post_id                        | object              | Computed post id by phoenix. This is the account platform id and the message hash. |
| account_platform_id                    | int64               | The facebook id of the account. |
| account_page_category                  | object              | The page category as submitted by the page.  |
| account_page_admin_top_country         | object              | The ISO country code of the the country from where the plurality of page administrators operate. |
| account_page_description               | object              | The description of the page as documented in Page Transparency information. |
| account_url                            | object              | URL of the account. |
| page_created                           | datetime64[ns, UTC] | The UTC timestamp on the creation of the page |
| account_subscriber_count               | int64               | Number of follower or likes when data pulled. Whether it is followers or likes is set in the crowdtangle dashboard. Default followers.  |
| subscriber_count                       | int64               | Number of follower or likes of account when post is created. Whether it is followers or likes is set in the crowdtangle dashboard. Default followers.  |
| post_created                           | datetime64[ns, UTC] | The UTC timestamp on the creation of the post |
| updated                                | datetime64[ns, UTC] | The UTC timestamp of the moment the post was updated in CrowdTangle |
| file_timestamp                         | datetime64[ns, UTC] | The timestamp that is on the source file. |
| type                                   | object              | The type of the post. |
| statistics_actual_like_count           | int64               | No. like reactions |
| statistics_actual_comment_count        | int64               | No. comments reactions |
| statistics_actual_share_count          | int64               | No. shares reactions |
| statistics_actual_love_count           | int64               | No. loves reactions |
| statistics_actual_wow_count            | int64               | No. wow reactions |
| statistics_actual_haha_count           | int64               | No. haha reactions |
| statistics_actual_sad_count            | int64               | No. sad reactions |
| statistics_actual_angry_count          | int64               | No. angry reactions
| statistics_actual_care_count           | int64               | No. care reactions |
| score                                  | float64             | Score from crowdtangle. Phoenix sets this as `total_interactions`. See scraping crowdtangle code for more information. |
| platform_id                            | float64             | Id of the post from facebook |
| video_length_ms                        | float64             | The length of the video in milliseconds. |
| id                                     | object              | Id of the post from crowdtangle |
| platform                               | object              | Platform id. |
| caption                                | object              | The caption to a photo, if available. |
| description                            | object              | Further details, if available. Associated with links or images across different platforms. |
| post_url                               | object              | URL of the post |
| langauge_code                          | object              | Language code from the API |
| message                                | object              | Text of the post |
| link                                   | object              | Link in the post |
| image_text                             | object              | Text of the image in the post if there is one |
| description                            | object              | Further details, if available. Associated with links or images across different platforms. |
| message_link                           | object              | Message or link if message is null  |
| message_hash                           | object              | The hash of the message or link used for calculating the `phoenix_post_id` |
| scrape_url                             | object              | The scrape url to be used |
| url_post_id                            | object              | The post id from the URL. |

# Facebook posts topics

This has the same data as facebook posts dataframe but has the columns as below.

There can be multiple topics for a facebook post in this case the facebook post data is repeated.

| Column                  | dtype          | Description |
|-------------------------|----------------|-------------|
| topic                   | object         | topic that phoenix found in the text |
