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
