from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from scripts.load_to_bigquery import load_to_bigquery
from scripts.move_to_gcs import move_file_to_gcs
from scripts.save_to_parquet import json_to_parquet

BUCKET_NAME = "dtc_data_lake_lunar-prism-390409"
TABLE = "lunar-prism-390409.github_archive.events"

with DAG(
    dag_id="github-archive",
    description="github archive pipeline",
    start_date=datetime(2023, 7, 31),
    schedule="@hourly",
    catchup=False,
) as dag:
    move_file_task = PythonOperator(
        task_id="move_file_to_gcs_task",
        python_callable=move_file_to_gcs,
        op_kwargs={"bucket_name": BUCKET_NAME},
        dag=dag,
    )

    save_to_parquet_task = PythonOperator(
        task_id="save_to_parquet",
        python_callable=json_to_parquet,
        op_kwargs={"bucket_name": BUCKET_NAME},
        dag=dag,
    )

    load_to_bigquery_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        op_kwargs={"table": TABLE, "bucket_name": BUCKET_NAME},
        dag=dag,
    )

    move_file_task >> save_to_parquet_task >> load_to_bigquery_task
