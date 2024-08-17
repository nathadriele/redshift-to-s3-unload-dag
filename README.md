## Redshift to S3 Unload DAG for Apache Airflow

This Airflow DAG automates the process of extracting data from an Amazon Redshift database and unloading it to Amazon S3 in Parquet format. It runs daily, exporting data from the previous day based on a specified query. 
