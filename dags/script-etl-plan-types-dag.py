from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.transfers.redshift_to_mysql import RedshiftToMySqlOperator

default_args = {
    "owner": "analytics",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 18),
    "email": ["alert@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Redshift SQL query
redshift_query = """
SELECT 
    c_id AS customer_id,
    discounts.cycles AS duration,
    CASE 
        WHEN duration > 10 AND duration < 14 THEN 'Annual'
        WHEN duration IN (3) THEN 'Quarterly'
        WHEN duration IN (5, 6) THEN 'Semiannual'
        ELSE 'Other'
    END AS plan_type,
    discounts.created_at
FROM recurrent.customers c
JOIN recurrent.subscriptions s ON s.customer_id = c.c_id
JOIN recurrent.subscription_discounts sd ON sd.subscription_id = s.s_id
JOIN recurrent.discounts d ON d.id = sd.discounts_id
JOIN recurrent.product_items PI ON PI.product_item_id = d.product_item_id
WHERE c.active = TRUE
  AND s.active = TRUE
  AND discounts.active = TRUE
  AND discounts.cycles IN (3, 5, 6, 10, 11, 12, 13)
  AND PI.product_name NOT IN (
      'Custom Domain',
      'Automatic Card Update',
      'Invoice',
      'Verification Transaction'
  )
GROUP BY c_id, discounts.cycles, discounts.created_at
HAVING COUNT(c_id) > 0;
"""

redshift_conn_id = "redshift_default"
mysql_conn_id = "mysql_default"

mysql_query = """
INSERT INTO staging_plan_types (customer_id, duration, plan_type, created_at) 
VALUES (%s, %s, %s, %s);
"""

with DAG(
    dag_id="plan_types_etl",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=["etl", "mysql", "redshift"],
) as dag:

    truncate_task = MySqlOperator(
        task_id="truncate_staging_table",
        mysql_conn_id=mysql_conn_id,
        sql="TRUNCATE TABLE staging_plan_types;",
    )

    # Task to transfer data from Redshift to MySQL
    etl_task = RedshiftToMySqlOperator(
        task_id="redshift_to_mysql_transfer",
        redshift_conn_id=redshift_conn_id,
        mysql_conn_id=mysql_conn_id,
        redshift_sql=redshift_query,
        mysql_table="staging_plan_types",
    )

    truncate_task >> etl_task