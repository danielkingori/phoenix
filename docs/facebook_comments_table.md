# Facebook Comments Table
Current the facebook comments table is initialised in athena by hand.

Once the tagging pipeline has been run:
- `phoenix/tag/data_pull/facebook_comments_pull_json.ipynb`
- `phoenix/tag/features.ipynb`, with the parameters:
```
# Set Artefacts URL
ARTIFACTS_BASE_URL = f"{artifacts.urls.get_local()}{RUN_DATE}/comments/"

# Input
FOR_TAGGING_ARTIFACTS_FOLDER = f"{ARTIFACTS_BASE_URL}comments_for_tagging/"
```
- `phoenix/tag/facebook_comments_finalise.ipynb`


The comments final data will be persisted to s3. See `phoenix/tag/facebook_comments_finalise.ipynb`
as to where exactly.

This will overwrite the persisted data each time it is run.

## Topics
The above pipeline will output:
`/comments/key_comments_features_to_label.csv`

This can be labelled by an analyst with topics:
`features`,`topic`
`string`, `t1,t2...`

Export this labelled data as a csv. It is then possible create a new config by merging the new and old mappings. Do this with the notebook:
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

This will persist the final data to s3. See notebooks for more details.

## Athena table
Was initialised using the command:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook_comments_may (
`id` bigint,
`post_id` bigint,
`file_id` string,
`parent_id` bigint,
`post_created` timestamp,
`text` string,
`reactions` int,
`top_sentiment_reactions` array<string>,
`user_display_name` string,
`username` string,
`position` string,
`clean_text` string,
`language` string,
`confidence` double,
`is_key_object` boolean,
`features` array<string>,
`features_count` array<int>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://buildup-dev-us-tables/fb_comments/parquet_exports/fb_comments_may/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
If you need to make changes:
```
DROP TABLE IF EXISTS buildup_dev.facebook_comments_may;
```

### Topics
Athena table created with query:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook_comments_topics_may (
`id` bigint,
`post_id` bigint,
`file_id` string,
`parent_id` bigint,
`post_created` timestamp,
`text` string,
`reactions` int,
`top_sentiment_reactions` array<string>,
`user_display_name` string,
`username` string,
`position` string,
`clean_text` string,
`language` string,
`confidence` double,
`is_key_object` boolean,
`features` array<string>,
`features_count` array<int>,
`topic` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://buildup-dev-us-tables/fb_comments/parquet_exports/fb_comments_topics_may/'
TBLPROPERTIES ('has_encrypted_data'='false');
```