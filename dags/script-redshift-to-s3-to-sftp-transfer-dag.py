from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_sftp import S3ToSFTPOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pendulum

redshift_conn_id = "redshift_conn_id"
yesterday = pendulum.yesterday().strftime("%Y-%m-%d")
bucket = Variable.get("S3_YOUR_BUCKET")
directory = "data_directory"

def unload_data():
    """
    Unloads data from Redshift to an S3 bucket.
    
    Constructs an UNLOAD SQL query to extract data from Redshift and write it
    to an S3 bucket in CSV format.
    """
    s3_path = f's3://{bucket}/{directory}/{yesterday}/*'
    iam_role = Variable.get("IAM_ROLE_DMS_ACCESS_FOR_ENDPOINT")
    
    query = f"""
    UNLOAD ('
    SELECT *
    FROM schema_name.table_name
    WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1 day'
    ')
    TO '{s3_path}'
    IAM_ROLE '{iam_role}'
    FORMAT CSV
    HEADER
    DELIMITER AS ','
    CLEANPATH
    PARALLEL OFF
    MAXFILESIZE AS 5 GB
    EXTENSION CSV;
    """

    postgres_operator = PostgresOperator(
        task_id='execute_query',
        sql=query,
        postgres_conn_id=redshift_conn_id,
        retries=0,
    )
    postgres_operator.execute(dict())

def upload_to_sftp():
    """
    Uploads files from S3 to an SFTP server.
    
    Lists files in the specified S3 bucket and directory, then uploads each file
    to an SFTP server.
    """
    path = f'{directory}/{yesterday}/*'
    s3_list_operator = S3ListOperator(
        task_id="list_files",
        bucket=bucket,
        prefix=path
    )
    files = s3_list_operator.execute(dict())
    
    for file in files:
        # Create and execute an S3ToSFTPOperator to upload each file to SFTP.
        sftp_operator = S3ToSFTPOperator(
            task_id="upload_file_to_sftp",
            sftp_conn_id='sftp_connection',
            sftp_path=f'{path}_data.csv',
            s3_bucket=bucket,
            s3_key=file
        )
        sftp_operator.execute(dict())

# Define the DAG for orchestrating the data pipeline.
with DAG(
    'data_pipeline_dag',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=["data_pipeline"],
    catchup=False    
) as dag:

# Task to unload data from Redshift to S3.
    unload_task = PythonOperator(
        task_id='unload_data_task',
        python_callable=unload_data,
        do_xcom_push=True,
    )

# Task to upload files from S3 to SFTP.
    upload_to_sftp_task = PythonOperator(
        task_id='upload_file_task',
        python_callable=upload_to_sftp,
        do_xcom_push=True,
    )

    unload_task >> upload_to_sftp_task
