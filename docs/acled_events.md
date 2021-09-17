# ACLED Events data
The ACLED events data is used to map events to social media interations.

Information about the data source:
https://acleddata.com/#/dashboard

The documentation on acled events is not the best. Here are some resources that I found:
- https://acleddata.com/resources/general-guides/
- [Data Columns](https://acleddata.com/acleddatanew/wp-content/uploads/dlm_uploads/2019/04/ACLED-Data-Columns_Quick-Reference_FINAL.pdf)
- [General Use](https://acleddata.com/acleddatanew/wp-content/uploads/dlm_uploads/2019/04/General-User-Guide_FINAL-1.pdf)

## How to
- Download the csv: https://acleddata.com/data-export-tool/
- Move the downloaded csv to the `s3://phoenix-data-lake-prod/base/acled_events/`
- Run the phoenix/scrape/acled_event_transform.ipynb notebook changing the `ARTIFACTS_ENVIRONMENT_KEY` to `production`
- This will create the correct data and persisted to the correct URL

Noted that this will partition by year and month, and is additive 
It should be that the data is automatically added to athena.

## Athena query for event data:
If you need to change the Athena table:

First Drop the existing table: `DROP TABLE IF EXISTS buildup_dev.acled_events;`

```
CREATE EXTERNAL TABLE IF NOT EXISTS buildup_dev.acled_events (
  `data_id` int,
  `event_id_cnty` string,
  `event_id_no_cnty` int,
  `event_date` string,
  `event_type` string,
  `sub_event_type`	string,
  `actor1` string,
  `assoc_actor_1` string,
  `actor2` string,
  `region` string,
  `country`	string,
  `location` string,
  `latitude` double,
  `longitude`	double,
  `source` string,
  `source_scale` string,
  `notes`	string,
  `fatalities` int,
  `timestamp`	bigint,
  `iso4` string,
  `timestamp_filter` TIMESTAMP,
  `date_filter` date,
  `year_filter` int,
  `month_filter` int,
  `day_filter` int,

)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://buildup-dev-us-tables/acled_events/'
TBLPROPERTIES ('has_encrypted_data'='false');
```
