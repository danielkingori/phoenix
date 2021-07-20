# Tweets table
Current the tweets table is initialised in athena by hand.

Once the tagging pipeline has been run:
- `phoenix/tag/data_pull/twitter_pull_json.ipynb`
- `phoenix/tag/features.ipynb`
- `phoenix/tag/twitter_facebook_posts_finalise.ipynb`


The tweets final data will be persisted to s3. See `phoenix/tag/twitter_facebook_posts_finalise.ipynb`
as to where exactly.

This will overwrite the persisted data each time it is run.

## Topics
The above pipeline will output:
`key_tweets_features_to_label.csv`

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
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.tweets_may (
  TODO: change to string
  `id_str` string,
  `created_at` timestamp,
  `id` bigint,
  TODO: change to `text`
  `text` string,
  `truncated` boolean,
  `source` string,
  `is_quote_status` boolean,
  `retweet_count` int,
  `favorite_count` int,
  `favorited` boolean,
  TODO: change to `language_from_api`
  `language_from_api` string,
  `possibly_sensitive` double,
  `clean_text` string,
  `language` string,
  TODO: change to `language_confidence`
  `language_confidence` double,
  `is_unofficial_retweet` boolean,
  `is_retweet` boolean,
  `is_key_object` boolean,
  `features` array<string>,
  `features_count` array<int>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
TODO: fix URL
) LOCATION 's3://buildup-dev-us-tables/tweets/parquet_exports/tweets/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
If you need to make changes:
```
DROP TABLE IF EXISTS buildup_dev.tweets;
```

### Topics
Athena table was created with the query:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.tweets_topics (
  TODO: Changes to match `tweets` table
  `id_str` string,
  `created_at` timestamp,
  `id` bigint,
  `text` string,
  `truncated` boolean,
  `source` string,
  `is_quote_status` boolean,
  `retweet_count` int,
  `favorite_count` int,
  `favorited` boolean,
  `language_from_api` string,
  `possibly_sensitive` double,
  `clean_text` string,
  `language` string,
  `language_confidence` double,
  `is_unofficial_retweet` boolean,
  `is_retweet` boolean,
  `is_key_object` boolean,
  `features` array<string>,
  `features_count` array<int>,
  `topic` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
TODO: fix URL
) LOCATION 's3://buildup-dev-us-tables/tweets/parquet_exports/tweets_topics/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
