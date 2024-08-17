from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pendulum

def get_aws_credentials():
    """Retrieve AWS credentials from Airflow."""
    return {
        'aws_access_key': Variable.get("aws_access_key_id"),
        'aws_secret_key': Variable.get("aws_secret_access_key")
    }

def generate_unload_query(schema, table_name, s3_path, aws_credentials):
    """Generate the UNLOAD SQL query."""
    return f"""
    UNLOAD ('SELECT * FROM "{schema}"."{table_name}" WHERE DATE(updated_at) = CURRENT_DATE - INTERVAL \'1 day\'')
    TO '{s3_path}'
    CREDENTIALS 'aws_access_key_id={aws_credentials['aws_access_key']};aws_secret_access_key={aws_credentials['aws_secret_key']}'
    FORMAT AS PARQUET
    PARALLEL ON
    CLEANPATH;
    """

# DAG configuration
dag = DAG(
    's3_unload_dag',
    schedule_interval='30 12 * * *',
    start_date=days_ago(1),
    catchup=False
)

# Retrieve AWS credentials
aws_credentials = get_aws_credentials()

yesterday = pendulum.yesterday().strftime('%Y-%m-%d')

s3_file_path = f's3://bucket-name/path/to/exported_data_{yesterday}.csv'

unload_query = generate_unload_query("schema", "table_name", s3_file_path, aws_credentials)

# Create a task to execute the UNLOAD
unload_task = PostgresOperator(
    task_id='execute_unload',
    sql=unload_query,
    postgres_conn_id='redshift_default_conn_id',
    params={'retries': 0},
    dag=dag,
)

# Set task dependencies if necessary
unload_task
