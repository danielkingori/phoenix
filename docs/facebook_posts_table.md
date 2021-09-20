# Facebook Posts Table
Current the facebook posts table is initialised in athena by hand.

Once the tagging pipeline has been run:
- `phoenix/tag/data_pull/facebook_posts_pull_csv.ipynb`
- `phoenix/tag/features.ipynb`
- `phoenix/tag/topics.ipynb`
- `phoenix/tag/tag_tensions.ipynb`
- `phoenix/tag/third_party_models/aws_async/start_sentiment.ipynb`
- Wait for the AWS comprehend job to finish. See [docs/language_sentiment_aws_comprehend.md](docs/language_sentiment_aws_comprehend.md).
- `phoenix/tag/third_party_models/aws_async/complete_sentiment.ipynb`
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
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook_posts_v1 (
`phoenix_post_id` string,
`account_handle` string,
`account_name` string,
`account_platform_id` bigint,
`account_page_category` string,
`account_page_admin_top_country` string,
`account_page_description` string,
`account_url` string,
`page_created` timestamp,
`account_subscriber_count` int,
`subscriber_count` int,
`post_created` timestamp,
`timestamp_filter` timestamp,
`date_filter` date,
`year_filter` int,
`month_filter` int,
`day_filter` int,
`updated` timestamp,
`file_timestamp` timestamp,
`type` string,
`statistics_actual_like_count` bigint,
`statistics_actual_comment_count` bigint,
`statistics_actual_share_count` bigint,
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
`language_from_api` string,
`text` string,
`link` string,
`image_text` string,
`scrape_url` string,
`url_post_id` string,
`clean_text` string,
`language` string,
`language_confidence` double,
`is_key_object` boolean,
`is_economic_labour_tension` boolean,
`is_sectarian_tension` boolean,
`is_environmental_tension` boolean,
`is_political_tension` boolean,
`is_service_related_tension` boolean,
`is_community_insecurity_tension` boolean,
`is_geopolitics_tension` boolean,
`is_intercommunity_relations_tension` boolean,
`has_tension` boolean,
`language_sentiment` string,
`language_sentiment_score_mixed` double,
`language_sentiment_score_neutral` double,
`language_sentiment_score_negative` double,
`language_sentiment_score_positive` double,
`features` array<string>,
`features_count` array<int>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://buildup-dev-us-tables/facebook_posts/parquet_exports/facebook_posts_v1/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
If you need to make changes:
```
DROP TABLE IF EXISTS buildup_dev.facebook_posts_v1;
```
### Topics
Athena table was created with the query:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook_posts_topics_v1 (
`phoenix_post_id` string,
`account_handle` string,
`account_name` string,
`account_platform_id` bigint,
`account_page_category` string,
`account_page_admin_top_country` string,
`account_page_description` string,
`account_url` string,
`page_created` timestamp,
`account_subscriber_count` int,
`subscriber_count` int,
`post_created` timestamp,
`timestamp_filter` timestamp,
`date_filter` date,
`year_filter` int,
`month_filter` int,
`day_filter` int,
`updated` timestamp,
`file_timestamp` timestamp,
`type` string,
`statistics_actual_like_count` bigint,
`statistics_actual_comment_count` bigint,
`statistics_actual_share_count` bigint,
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
`language_from_api` string,
`text` string,
`link` string,
`image_text` string,
`scrape_url` string,
`url_post_id` string,
`clean_text` string,
`language` string,
`language_confidence` double,
`is_key_object` boolean,
`is_economic_labour_tension` boolean,
`is_sectarian_tension` boolean,
`is_environmental_tension` boolean,
`is_political_tension` boolean,
`is_service_related_tension` boolean,
`is_community_insecurity_tension` boolean,
`is_geopolitics_tension` boolean,
`is_intercommunity_relations_tension` boolean,
`has_tension` boolean,
`language_sentiment` string,
`language_sentiment_score_mixed` double,
`language_sentiment_score_neutral` double,
`language_sentiment_score_negative` double,
`language_sentiment_score_positive` double,
`features` array<string>,
`features_count` array<int>,
`topic` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://buildup-dev-us-tables/facebook_posts/parquet_exports/facebook_posts_topics_v1/'
TBLPROPERTIES ('has_encrypted_data'='false');
```

# Athena Views
## Emotion view
Creates a view that allows the dashboard to aggregate emoji reaction counts per topic.

```
CREATE OR REPLACE VIEW facebook_posts_topics_emotions AS
 SELECT
  t1.phoenix_post_id
, t1.topic
, t1.year_filter
, t1.month_filter
, t1.is_economic_labour_tension
, t1.is_sectarian_tension
, t1.is_environmental_tension
, t1.is_political_tension
, t1.is_service_related_tension
, t1.is_community_insecurity_tension
, t1.is_geopolitics_tension
, t1.is_intercommunity_relations_tension
, t1.has_tension
, t1.language_sentiment
, t2.emotion_key
, t2.emotion_count
FROM
  (facebook_posts_topics_v1 t1
CROSS JOIN UNNEST(ARRAY['angry','care','haha','like','love','sad','wow'], ARRAY[statistics_actual_angry_count,statistics_actual_care_count,statistics_actual_haha_count,statistics_actual_like_count,statistics_actual_love_count,statistics_actual_sad_count,statistics_actual_wow_count]) t2 (emotion_key, emotion_count))
```
