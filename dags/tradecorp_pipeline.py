from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

default_args = {
    "owner": "tradecorp",
    "retries": 1,  # réessayer 1 fois en cas d'échec
    "retry_delay": timedelta(minutes=5),  # attendre 5 min avant de réessayer
}

mount_list = [
    Mount(
        source="//c/Users/Utilisateur/TP-CODE/SPARK/src",
        target="/home/jovyan/src",
        type="bind",
    ),
    Mount(
        source="//c/Users/Utilisateur/TP-CODE/SPARK/data",
        target="/home/jovyan/data",
        type="bind",
    ),
    Mount(
        source="//c/Users/Utilisateur/TP-CODE/SPARK/.env",
        target="/home/jovyan/.env",
        type="bind",
    ),
]

with DAG(
    dag_id="tradecorp_etl_pipeline",  # nom unique du DAG
    default_args=default_args,
    start_date=datetime(2026, 9, 1),  # date à partir de laquelle il "existe"
    schedule_interval="0 6 * * *",  # cron : tous les jours à 6h (UTC)
    catchup=False,  # ne pas rattraper le passé
    tags=["tradecorp", "etl", "spark"],
) as dag:

    fetch_exchange_rates = BashOperator(
        task_id="fetch_exchange_rates",
        bash_command="python3 /opt/airflow/src/fetch_exchange_rates.py",
    )

    reader = DockerOperator(
        task_id="reader",
        image="tradecorp-spark",
        force_pull=False,
        command="spark-submit /home/jovyan/src/reader.py",
        docker_url="unix:///var/run/docker.sock",
        network_mode="tradecorp_default",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=mount_list,
    )

    transformer = DockerOperator(
        task_id="transformer",
        image="tradecorp-spark",
        command="spark-submit /home/jovyan/src/transformer.py",
        docker_url="unix:///var/run/docker.sock",
        network_mode="tradecorp_default",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=mount_list,
    )

    writer = DockerOperator(
        task_id="writer",
        image="tradecorp-spark",
        command="spark-submit /home/jovyan/src/writer.py",
        docker_url="unix:///var/run/docker.sock",
        network_mode="tradecorp_default",
        mount_tmp_dir=False,
        auto_remove=True,
        mounts=mount_list,
    )

    fetch_exchange_rates >> reader >> transformer >> writer
