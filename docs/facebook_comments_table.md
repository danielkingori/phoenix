# Facebook Comments Table
Currently the facebook comments table is initialised in Athena by hand.

Once the tagging pipeline has been run:
- `phoenix/tag/data_pull/facebook_comments_pull_json.ipynb`
- `phoenix/tag/features.ipynb`, with the parameters:
```
# Set Artefacts URL
ARTIFACTS_BASE_URL = f"{artifacts.urls.get_local()}{RUN_DATE}/comments/"

# Input
FOR_TAGGING_ARTIFACTS_FOLDER = f"{ARTIFACTS_BASE_URL}comments_for_tagging/"
```
- `phoenix/tag/topics.ipynb`
- `phoenix/tag/tag_tensions.ipynb`
- `phoenix/tag/third_party_models/aws_async/start_sentiment.ipynb`
- Wait for the AWS comprehend job to finish. See [docs/language_sentiment_aws_comprehend.md](docs/language_sentiment_aws_comprehend.md).
- `phoenix/tag/third_party_models/aws_async/complete_sentiment.ipynb`
- `phoenix/tag/twitter_facebook_posts_finalise.ipynb`
- `phoenix/tag/facebook_comments_finalise.ipynb`


The comments final data will be persisted to S3. See `phoenix/tag/facebook_comments_finalise.ipynb`
as to where exactly.

This will overwrite the persisted data each time it is run.

## Topics
The above pipeline will output:
`/comments/key_comments_features_to_label.csv`

This can be labelled by an analyst with topics:
`features`,`topic`
`string`, `t1,t2...`

Export this labelled data as a CSV. It is then possible create a new config by merging the new and old mappings. Do this with the notebook:
`phoenix/tag/topic/single_feature_match_topic_config_process.ipynb`


To create the final topic data:
- `phoenix/tag/topics.ipynb`, with parameters:
```
# Set Artefacts URL
ARTIFACTS_BASE_URL = f"{artifacts.urls.get_local()}{RUN_DATE}/comments/"

# Input
ALL_FEATURES = artifacts.dataframes.url(ARTIFACTS_BASE_URL, "all_features")
```
- `phoenix/tag/facebook_comments_finalise_topics.ipynb`

This will persist the final data to S3. See notebooks for more details.

## Athena table
Was initialised using the command:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook_comments_v1 (
`id` bigint,
`post_id` bigint,
`file_id` string,
`parent_id` bigint,
`post_created` timestamp,
`timestamp_filter` timestamp,
`date_filter` date,
`year_filter` int,
`month_filter` int,
`day_filter` int,
`text` string,
`reactions` int,
`top_sentiment_reactions` array<string>,
`user_display_name` string,
`user_name` string,
`position` string,
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
) LOCATION 's3://buildup-dev-us-tables/facebook_comments/parquet_exports/facebook_comments_v1/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
If you need to make changes:
```
DROP TABLE IF EXISTS buildup_dev.facebook_comments;
```

### Topics
Athena table created with query:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook_comments_topics_v1 (
`id` bigint,
`post_id` bigint,
`file_id` string,
`parent_id` bigint,
`post_created` timestamp,
`timestamp_filter` timestamp,
`date_filter` date,
`year_filter` int,
`month_filter` int,
`day_filter` int,
`text` string,
`reactions` int,
`top_sentiment_reactions` array<string>,
`user_display_name` string,
`user_name` string,
`position` string,
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
) LOCATION 's3://buildup-dev-us-tables/facebook_comments/parquet_exports/facebook_comments_topics_v1/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
