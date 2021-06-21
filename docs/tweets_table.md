# Tweets table
Current the tweets table is initialised in athena by hand.

Once the tagging pipeline has been run:
- `phoenix/tag/data_pull/twitter_pull_json.ipynb`
- `phoenix/tag/features.ipynb`
- `phoenix/tag/twitter_facebook_posts_finalise.ipynb`


The tweets final data will be persisted to s3. See `phoenix/tag/twitter_facebook_posts_finalise.ipynb`
as to where exactly.

This will overwrite the persisted data each time it is run.

## Athena table
Was initialised using the command:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.tweets_may (
  `id_str` bigint,
  `created_at` timestamp,
  `id` bigint,
  `full_text` string,
  `truncated` boolean,
  `source` string,
  `is_quote_status` boolean,
  `retweet_count` int,
  `favorite_count` int,
  `favorited` boolean,
  `lang` string,
  `possibly_sensitive` double,
  `clean_text` string,
  `language` string,
  `confidence` double,
  `is_unofficial_retweet` boolean,
  `is_retweet` boolean,
  `is_key_object` boolean,
  `features` array<string>,
  `features_count` array<int>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://buildup-dev-us-tables/tweets/parquet_exports/tweets_may/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
If you need to make changes:
```
DROP TABLE IF EXISTS buildup_dev.tweets_may;
```
