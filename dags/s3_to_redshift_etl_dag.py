from datetime import datetime
from os import getenv
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

S3_BUCKET = Variable.get("S3_BUCKET")
S3_KEY = Variable.get("S3_KEY")
REDSHIFT_TABLE = Variable.get("REDSHIFT_TABLE")

with DAG(
    dag_id="s3_to_redshift",
    start_date=datetime(2024, 9, 5),
    schedule_interval=None,
    catchup=False,
) as dag:

    begin = DummyOperator(
        task_id="begin"
    )

    # Task to transfer data from S3 to Redshift
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id='transfer_s3_to_redshift',
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema="public",
        table=REDSHIFT_TABLE,
        copy_options=['csv'],
    )

    end = DummyOperator(
        task_id="end"
    )

    chain(
        begin,
        task_transfer_s3_to_redshift,
        end
    )
