from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.dms import DmsStartTaskOperator
from airflow.providers.amazon.aws.sensors.dms import DmsTaskBaseSensor
from datetime import datetime, timedelta

DMS_TASK_ARN = Variable.get("DMS_TASK_ARN")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 12, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dms_full_load",
    schedule_interval="0 1 * * *",
    default_args=default_args,
    catchup=False,
    tags=["dms", "full_refresh"],
) as dag:

    start_full_refresh = DmsStartTaskOperator(
        task_id="start_dms_task",
        replication_task_arn=DMS_TASK_ARN,
        start_replication_task_type="reload-target",
    )

    monitor_full_refresh = DmsTaskBaseSensor(
        task_id="monitor_dms_task",
        replication_task_arn=DMS_TASK_ARN,
        poke_interval=60,
        timeout=3600,
    )

    start_full_refresh >> monitor_full_refresh