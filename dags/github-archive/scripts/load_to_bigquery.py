from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


def load_to_bigquery(table: str, bucket_name: str, **kwargs):
    ti = kwargs["ti"]
    filename = ti.xcom_pull(key="filename")

    bigquery_hook = BigQueryHook()
    bigquery_hook.run_load(
        destination_project_dataset_table=table,
        source_uris=[f"gs://{bucket_name}/parquet/{filename}/*.parquet"],
        autodetect=True,
        source_format="Parquet",
        write_disposition="WRITE_APPEND",
    )
