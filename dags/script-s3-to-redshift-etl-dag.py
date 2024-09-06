from datetime import datetime
import logging
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_variable(key, default_value=None):
    """
    Utility function to fetch Airflow variables with a default fallback.
    This prevents the DAG from failing if the variable is not set.
    """
    try:
        return Variable.get(key)
    except KeyError:
        logger.warning(f"Variable {key} not found, using default value: {default_value}")
        return default_value

def check_connections(**kwargs):
    redshift_conn_id = get_variable("REDSHIFT_CONN_ID", "redshift_default")
    aws_conn_id = get_variable("AWS_CONN_ID", "aws_default")
    
S3_BUCKET = get_variable("S3_BUCKET", "default-bucket")
S3_KEY = get_variable("S3_KEY", "default-key")
REDSHIFT_TABLE = get_variable("REDSHIFT_TABLE", "default_table")
REDSHIFT_SCHEMA = get_variable("REDSHIFT_SCHEMA", "public")
COPY_OPTIONS = get_variable("COPY_OPTIONS", ['csv']).split(',')

with DAG(
    dag_id="s3_to_redshift",
    description="DAG to transfer data from S3 to Redshift",
    start_date=datetime(2024, 9, 5),
    schedule_interval=None,
    catchup=False,
    tags=["data-transfer", "redshift", "s3"],
) as dag:

    begin = EmptyOperator(
        task_id="begin",
        dag=dag
    )

    check_connections_task = EmptyOperator(
        task_id="check_connections",
        dag=dag,
        on_failure_callback=check_connections
    )

    # Task to transfer data from S3 to Redshift
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id='transfer_s3_to_redshift',
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema=REDSHIFT_SCHEMA,
        table=REDSHIFT_TABLE,
        redshift_conn_id=get_variable("REDSHIFT_CONN_ID", "redshift_default"),
        aws_conn_id=get_variable("AWS_CONN_ID", "aws_default"),
    )

    logger.info(f"Starting transfer from {S3_BUCKET}/{S3_KEY} to Redshift table {REDSHIFT_TABLE}")

    end = EmptyOperator(
        task_id="end",
        dag=dag
    )

    chain(
        begin,
        check_connections_task,
        task_transfer_s3_to_redshift,
        end
    )

    logger.info("DAG s3_to_redshift defined successfully")