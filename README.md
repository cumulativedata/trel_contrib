# Trel community code base

Here you will find a number of jobs, sensors and their registrations that you can use in your pipeline / warehouse.

`ET Inv` column indicates whether these sensors are execution-time invariant. With this property, sensors when delayed can back-fill the missing datasets with data identical to what they would contain if no delays ocurred. Without this property, delays can influence the data in the datasets.

## Sensors

| sensor | group | ET Inv | description |
| ------ | ----- | ------ | ----------- |
| odbc_table_load | bigquery, s3 |  | Periodically load a table from an ODBC data source into target repository |
| twitter_s3 | s3 | Yes | Pull tweets from Twitter broken into configurable time windows and insert into target repository |
| finnhub | bigquery | Yes | Pull stock-tick information from Finnhub broken into configurable time windows and insert into target repository |
| bigquery_table | bigquery | Yes | Can monitor a dataset in bigquery for new tables and add them to the catalog. |
| local_file | test | Yes | Can detect new files within the VM running Trel. Only useful for testing. |

## Jobs

| job | group | ET Inv | description |
| --- | ----- | ------ | ----------- |
| append_day | bigquery, demo | Yes | A bigquery code part of the Trel demo |
| report_summary | bigquery, demo | Yes | A bigquery code part of the Trel demo |
| lifecycle/s3_python (python/pyspark) | lifecycle | Yes | This job will perform lifecycle on S3 type repositories |
| lifecycle/gcp (python/pyspark) | lifecycle | Yes | This job will perform lifecycle on Google Storage and BigQuery repositories |
