## Redshift to S3 Unload DAG for Apache Airflow

### Overview

This project is an Apache Airflow DAG designed to automate the process of extracting data from an Amazon Redshift database and unloading it to Amazon S3 in Parquet format. The DAG is scheduled to run daily, exporting data from the previous day based on a specified SQL query.

### Prerequisites

Before using this project, ensure that the following prerequisites are met:

- `Apache Airflow` is installed and configured.
- `Amazon Redshift` connection configured in Airflow.
- `Amazon S3` bucket accessible for storing the data.
- `AWS Credentials` stored as Airflow Variables: `aws_access_key_id` and `aws_secret_access_key`
- `Python` 3.6+ (for compatibility with Airflow and dependencies).

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

#### 1. Retrieve AWS Credentials

This function retrieves AWS credentials stored in Airflow variables, ensuring sensitive information is handled securely.

#### 2. Generate UNLOAD SQL Query

This function constructs the SQL query for unloading data from Redshift to S3, incorporating credentials and path parameters.

#### 3. Create DAG and Task

This section creates an Airflow DAG scheduled to run (exampple) daily at 12:30 PM UTC. catchup=False prevents backfilling of missed runs. This task executes the UNLOAD query on Redshift, storing the data in S3 as specified.

### Testing

1. Manual Verification

After setting up the DAG, manually trigger it via the Airflow interface to ensure it runs as expected.

2. Log Monitoring

Check Airflow logs during execution to verify:

- Successful connection to Redshift.
- Proper execution of the UNLOAD query.
- Correct file creation in S3 with the expected name and format.

### Result

The data from the specified Redshift table will be unloaded to the S3 bucket in Parquet format, with filenames including the previous day's date. Verify the output by accessing the S3 bucket.

### Contribution to Data Engineering

This project provides a practical solution for automating data exports from Redshift to S3, enhancing data archiving and migration processes. The code's modularity and security features make it adaptable for various use cases in data engineering.

### Best Practices

- `Security`: Using Airflow variables for AWS credentials ensures sensitive information is not hardcoded.
- `Modularity`: Separating credential retrieval and query generation enhances code clarity and reuse.
- `Flexibility`: Configurable schema, table names, and S3 paths allow the code to be easily adapted for different scenarios.
