from airflow import DAG

from fivetran_provider.operators.fivetran import FivetranOperator
from fivetran_provider_async.sensors.fivetran import FivetranSensorAsync

from datetime import datetime, timedelta


default_args = {
    "owner": "Airflow",
    "start_date": datetime(2022, 4, 14),
}

dag = DAG(
    dag_id='example_fivetran_async',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with dag:
    fivetran_sync_start = FivetranOperator(
        task_id='fivetran-task',
        fivetran_conn_id='fivetran_default',
        connector_id="{{ var.value.connector_id }}",
    )

    fivetran_sync_wait = FivetranSensorAsync(
        task_id='fivetran-sensor',
        fivetran_conn_id='fivetran_default',
        connector_id="{{ var.value.connector_id }}",
        poke_interval=5,
    )

    fivetran_sync_start >> fivetran_sync_wait