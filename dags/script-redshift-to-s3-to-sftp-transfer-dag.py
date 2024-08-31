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
bucket = Variable.get("BUCKET")
directory = "directory"

def unload_data():
    s3_path = f's3://{bucket}/{directory}/{yesterday}/*'
    iam_role = Variable.get("IAM")
    
    query = f"""
    UNLOAD ('
    SELECT *
    FROM schema_name.table_name
    WHERE DATE(date) = CURRENT_DATE - INTERVAL '1 day'
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
    )
    postgres_operator.execute(dict())

def upload_to_sftp():
    path = f'{directory}/{yesterday}/*'
    s3_list_operator = S3ListOperator(
        task_id="list_files",
        bucket=bucket,
        prefix=path
    )
    files = s3_list_operator.execute(dict())
    
    for file in files:
        sftp_operator = S3ToSFTPOperator(
            task_id="upload_file_to_sftp",
            sftp_conn_id='sftp_connection',
            s3_bucket=bucket,
            s3_key=file
        )
        sftp_operator.execute(dict())

with DAG(
    'data_pipeline_dag',
    schedule_interval='@daily',
    start_date=days_ago(1),
) as dag:

    unload_task = PythonOperator(
        task_id='unload_data_task',
        python_callable=unload_data,
    )

    upload_to_sftp_task = PythonOperator(
        task_id='upload_file_task',
        python_callable=upload_to_sftp,
    )

    unload_task >> upload_to_sftp_task
