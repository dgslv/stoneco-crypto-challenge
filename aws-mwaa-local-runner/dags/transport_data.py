from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    "owner": "diegosilva",
    "depends_on_past": True,
    "start_date": datetime(2023, 9, 1, 8, 0, 0),
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    'stone_co_crypto_data_etl_challenge',
    default_args=default_args,
    description="Olha aqui",
    schedule_interval='0 8 * * *',
    catchup=True
) as dag:
    k = KubernetesPodOperator(
        namespace='default',
        image="stone",
        labels={"owner": default_args.get('owner')},
        name="mwaa-airflow-stoneco-pod",
        image_pull_policy="Never",
        task_id='stoneco_crypto_job',
        cluster_context="docker-desktop",  # is ignored when in_cluster is set to True
        is_delete_operator_pod=False,
        get_logs=True,
        env_vars={
            'PROCESSING_TIMESTAMP': '{{ logical_date }}',
            'GCP_CREDENTIALS_PATH': Variable.get('GCP_CREDENTIALS_PATH'),
            'POSTGRES_HOST': Variable.get('POSTGRES_STONECO_HOST'),
            'POSTGRES_PORT': Variable.get('POSTGRES_STONECO_PORT'),
            'POSTGRES_DB': Variable.get('POSTGRES_STONECO_DB'),
            'POSTGRES_USER': Variable.get('POSTGRES_STONECO_USER'),
            'POSTGRES_PASS': Variable.get('POSTGRES_STONECO_PASS'),
            'CRIPTO_ETL_RECORD_LIMIT': Variable.get('CRIPTO_ETL_RECORD_LIMIT')
        },
        in_cluster=False
    )

    k
