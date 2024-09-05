from datetime import datetime
import logging
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_variable(key, default_value=None):
    try:
        return Variable.get(key)
    except KeyError:
        logger.warning(f"Variable {key} not found, using default value: {default_value}")
        return default_value

S3_BUCKET = get_variable("S3_BUCKET", "default-bucket")
S3_KEY = get_variable("S3_KEY", "default-key")
REDSHIFT_TABLE = get_variable("REDSHIFT_TABLE", "default_table")

with DAG(
    dag_id="s3_to_redshift",
    start_date=datetime(2022, 3, 1),
    schedule_interval=None,
) as dag:

    # Dummy start task
    begin = DummyOperator(
        task_id="begin",
        dag=dag
    )

    # Task to transfer data from S3 to Redshift with logging
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id='transfer_s3_to_redshift',
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema="public",
        table=REDSHIFT_TABLE,
        copy_options=['csv'],
    )

    logger.info(f"Starting transfer from {S3_BUCKET}/{S3_KEY} to Redshift table {REDSHIFT_TABLE}")

    end = DummyOperator(
        task_id="end",
        dag=dag
    )

    chain(
        begin,
        task_transfer_s3_to_redshift,
        end
    )

    logger.info("DAG s3_to_redshift defined successfully")
