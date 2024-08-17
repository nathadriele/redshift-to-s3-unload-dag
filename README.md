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

### Installation

To set up and use this DAG in your Airflow instance, follow these steps:

1. Clone this repository:

```py
git clone https://github.com/nathadriele/redshift-to-s3-unload-dag.git
```

2. Copy the DAG to Airflow's DAGs folder:

```py
cp your_repository/redshift_to_s3_unload_dag.py $AIRFLOW_HOME/dags/
```

3. Configure the necessary environment variables and connections in Airflow for Redshift and AWS.

### Usage

Once installed, the DAG will be automatically available in the Airflow interface. To manually trigger or monitor it:

1. Access the Airflow web interface via your browser.
2. Locate the DAG named `redshift_to_s3_unload_dag`.
3. Click on Trigger DAG to run it manually.

The DAG will unload data from the specified Redshift table to the configured S3 bucket, with the filename including the previous day's date.

