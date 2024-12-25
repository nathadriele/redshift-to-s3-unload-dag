from datetime import datetime
from os import getenv
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy import DummyOperator

S3_BUCKET = Variable.get("S3_BUCKET")
S3_KEY = Variable.get("S3_KEY")
REDSHIFT_TABLE = Variable.get("REDSHIFT_TABLE")

with DAG(
    dag_id="s3_to_redshift",
    start_date=datetime(2024, 12, 20),
    schedule_interval=None,
    catchup=False,
    description="A DAG to transfer data from S3 to Redshift",
) as dag:

    start = DummyOperator(task_id="start")

    # Task to transfer data from S3 to Redshift
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift",
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema="public",
        table=REDSHIFT_TABLE,
        copy_options=["CSV"],
    )

    end = DummyOperator(task_id="end")

    chain(start, task_transfer_s3_to_redshift, end)