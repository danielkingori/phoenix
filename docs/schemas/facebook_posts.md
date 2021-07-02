# Facebook posts dataframe
 
Final dataframe of facebook posts.

| Column name                                                                                                | dtype               | Description                                                                                                       | 
|------------------------------------------------------------------------------------------------------------|---------------------| ------------------------------------------------------------------------------------------------------------------|
| phoenix_post_id                                                                                            | object              | Computed post id by phoenix. This is because csv data from crowdtangle has no post id. |
| page_name                                                                                                  | object              | Name of the page. |
| user_name                                                                                                  | object              | Name of the user that posted. |
| facebook_id                                                                                                | int64               | The id of the account. |
| page_category                                                                                              | object              | The category of the page |
| page_admin_top_country                                                                                     | object              | - |
| page_description                                                                                           | object              | - |
| page_created                                                                                               | datetime64[ns, UTC] | The UTC timestamp on the creation of the page |
| likes_at_posting                                                                                           | int64               | Number of likes at posting |
| followers_at_posting                                                                                       | int64               | Number of follower at posting |
| post_created                                                                                               | datetime64[ns, UTC] | The UTC timestamp on the creation of the post |
| post_created_date                                                                                          | object              | Not to be used |
| post_created_time                                                                                          | object              | Not to be used |
| type                                                                                                       | object              | - |
| total_interactions                                                                                         | int64               | Total interactions on the posts, this is the key metric for understanding the performance of the post |
| likes                                                                                                      | int64               | No. like reactions |
| comments                                                                                                   | int64               | No. comments reactions |
| shares                                                                                                     | int64               | No. shares reactions |
| love                                                                                                       | int64               | No. loves reactions |
| wow                                                                                                        | int64               | No. wow reactions |
| haha                                                                                                       | int64               | No. haha reactions |
| sad                                                                                                        | int64               | No. sad reactions |
| angry                                                                                                      | int64               | No. angry reactions
| care                                                                                                       | int64               | No. care reactions |
| video_share_status                                                                                         | object              | - |
| is_video_owner_                                                                                            | object              | No. posts views |
| post_views                                                                                                 | int64               | - |
| total_views                                                                                                | int64               | - |  
| total_views_for_all_crossposts                                                                             | int64               | - | 
| video_length                                                                                               | object              | URL of post from crowdtangle |
| url                                                                                                        | object              | Text of post |
| message                                                                                                    | object              | Links in the post |
| link                                                                                                       | object              |
| final_link                                                                                                 | object              | Text of the image in the post if there is one |
| image_text                                                                                                 | object              | - |
| link_text                                                                                                  | object              | - |
| description                                                                                                | object              | - |
| sponsor_id                                                                                                 | float64             | - |
| sponsor_name                                                                                               | float64             | - |
| sponsor_category                                                                                           | float64             | - |
| total_interactions_weighted_likes_1x_shares_1x_comments_1x_love_1x_wow_1x_haha_1x_sad_1x_angry_1x_care_1x_ | object              | - |
| overperforming_score                                                                                       | float64             | - |
| overperforming_score_weighted_likes_1x_shares_1x_comments_1x_love_1x_wow_1x_haha_1x_sad_1x_angry_1x_       | float64             | - |
| page_created_back                                                                                          | object              | - |
| page_created_utc                                                                                           | object              | - |
| post_created_back                                                                                          | object              | - |
| post_created_utc                                                                                           | object              | - |
| message_link                                                                                               | object              | - |
| message_hash                                                                                               | object              | The hash of the message used for calculating the `phoenix_post_id` |
| scrape_url                                                                                                 | object              | The scrape url to be used |
| url_post_id                                                                                                | object              | The post id from the URL. |

# Facebook posts topics

This has the same data as facebook posts dataframe but has the columns as below.

There can be multiple topics for a facebook post in this case the facebook post data is repeated.

| Column                  | dtype          | Description |
|-------------------------|----------------|-------------|
| topic                   | object         | topic that phoenix found in the text |
