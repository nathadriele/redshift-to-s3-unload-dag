## Redshift to S3 Unload DAG for Apache Airflow

### Overview

This project is an Apache Airflow DAG designed to automate the process of extracting data from an Amazon Redshift database and unloading it to Amazon S3 in Parquet format. The DAG is scheduled to run daily, exporting data from the previous day based on a specified SQL query.

### Prerequisites

Before using this project, ensure that the following prerequisites are met:

- Apache Airflow is installed and configured.
- Amazon Redshift connection configured in Airflow.
- Amazon S3 bucket accessible for storing the data.
- AWS Credentials stored as Airflow Variables: "aws_access_key_id" and "aws_secret_access_key"
- Python 3.6+ (for compatibility with Airflow and dependencies).
