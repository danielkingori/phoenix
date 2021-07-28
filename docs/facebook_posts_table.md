# Facebook Posts Table
Current the facebook posts table is initialised in athena by hand.

Once the tagging pipeline has been run:
- `phoenix/tag/data_pull/facebook_posts_pull_csv.ipynb`
- `phoenix/tag/features.ipynb`
- `phoenix/tag/twitter_facebook_posts_finalise.ipynb`


The facebook posts final data will be persisted to s3. See `phoenix/tag/twitter_facebook_posts_finalise.ipynb`
as to where exactly.

This will overwrite the persisted data each time it is run.

## Topics
The above pipeline will output:
`key_facebook_posts_features_to_label.csv`

This can be labelled by an analyst with topics:
`features`,`topic`
`string`, `t1,t2...`

Export this labelled data as a csv. It is then possible create a new config by merging the new and old mappings. Do this with the notebook:
`phoenix/tag/topic/single_feature_match_topic_config_process.ipynb`


To create the final topic data:
- `phoenix/tag/topics.ipynb`
- `phoenix/tag/twitter_facebook_posts_finalise_topics.ipynb`

This will persist the final data to s3. See notebooks for more details.


## Athena table
Was initialised using the command:
```
TODO: refactor to match the new facebook schema
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook (
`phoenix_post_id` string,
`account_handle` string,
`account_name` string,
`account_platform_id` int,
`account_page_category` string,
`account_page_admin_top_country` string,
`account_page_description` string,
`account_url` string,
`page_created` timestamp,
`account_subscriber_count` int,
`subscriber_count` int,
`post_created` timestamp,
`updated` timestamp,
`file_timestamp` timestamp,
`type` string,
`statistics_actual_likes_count` bigint,
`statistics_actual_comments_count` bigint,
`statistics_actual_shares_count` bigint,
`statistics_actual_love_count` bigint,
`statistics_actual_wow_count` bigint,
`statistics_actual_haha_count` bigint,
`statistics_actual_sad_count` bigint,
`statistics_actual_angry_count` bigint,
`statistics_actual_care_count` bigint,
`total_interactions` double,
`overperforming_score` double,
`interaction_rate` double,
`underperforming_score` double,
`platform_id` double,
`video_length_ms` double,
`id` string,
`platform` string,
`caption` string,
`description` string,
`post_url` string,
TODO: change this in facebook schema
`language_from_api` string,
`message` string,
`link` string,
`image_text` string,
`scrape_url` string,
`url_post_id` string,
`clean_text` string,
`language` string,
`language_confidence` double,
`is_key_object` boolean,
`features` array<string>,
`features_count` array<int>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
TODO: fix url
) LOCATION 's3://buildup-dev-us-tables/facebook_posts/parquet_exports/facebook_posts/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
If you need to make changes:
```
DROP TABLE IF EXISTS buildup_dev.facebook_posts;
```
### Topics
Athena table was created with the query:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook_posts_topics (
`phoenix_post_id` string,
`account_platform_id` int,
`account_page_category` string,
`account_page_admin_top_country` string,
`account_page_description` string,
`account_url` string,
`page_created` timestamp,
`account_subscriber_count` int,
`subscriber_count` int,
`post_created` timestamp,
`updated` timestamp,
`file_timestamp` timestamp,
`type` string,
`statistics_actual_likes_count` bigint,
`statistics_actual_comments_count` bigint,
`statistics_actual_shares_count` bigint,
`statistics_actual_love_count` bigint,
`statistics_actual_wow_count` bigint,
`statistics_actual_haha_count` bigint,
`statistics_actual_sad_count` bigint,
`statistics_actual_angry_count` bigint,
`statistics_actual_care_count` bigint,
`score` bigint,
`platform_id` double,
`video_length_ms` double,
`id` string,
`platform` string,
`caption` string,
`description` string,
`post_url` string,
`language_from_api` string,
`message` string,
`link` string,
`image_text` string,
`scrape_url` string,
`url_post_id` string,
`clean_text` string,
`language` string,
`language_confidence` double,
`is_key_object` boolean,
`features` array<string>,
`features_count` array<int>
`topic` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
TODO: fix this URL
) LOCATION 's3://buildup-dev-us-tables/facebook_posts/parquet_exports/facebook_posts_topics/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
