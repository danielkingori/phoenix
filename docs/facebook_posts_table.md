# Facebook Posts Table
Current the facebook posts table is initialised in athena by hand.

Once the tagging pipeline has been run:
- `phoenix/tag/data_pull/facebook_posts_pull_csv.ipynb`
- `phoenix/tag/features.ipynb`
- `phoenix/tag/twitter_facebook_posts_finalise.ipynb`


The tweets final data will be persisted to s3. See `phoenix/tag/twitter_facebook_posts_finalise.ipynb`
as to where exactly.

This will overwrite the persisted data each time it is run.

## Athena table
Was initialised using the command:
```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.facebook_posts_may (
`phoenix_post_id` string,
`page_name` string,
`user_name` string,
`page_admin_top_country` string,
`page_description` string,
`page_created` timestamp,
`likes_at_posting` int,
`followers_at_posting` int,
`post_created` timestamp,
`post_created_date` int,
`post_created_time` string,
`type` string,
`total_interactions` bigint,
`likes` bigint,
`comments` bigint,
`shares` bigint,
`love` bigint,
`wow` bigint,
`haha` bigint,
`sad` bigint,
`angry` bigint,
`care` bigint,
`video_share_status` string,
`is_video_owner` string,
`post_views` bigint,
`total_views` bigint,
`total_views_for_all_crossposts` bigint,
`video_length` string,
`url` string,
`message` string,
`link` string,
`final_link` string,
`image_text` string,
`link_text` string,
`description` string,
`sponsor_id` double,
`sponsor_name` double,
`sponsor_category` double,
`overperforming_score` double,
`scrape_url` string,
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
) LOCATION 's3://buildup-dev-us-tables/fb_posts/parquet_exports/fb_posts_may/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
If you need to make changes:
```
DROP TABLE IF EXISTS buildup_dev.facebook_posts_may;
```
