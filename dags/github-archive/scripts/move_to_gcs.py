import logging
from datetime import datetime, timedelta

import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook


def move_file_to_gcs(bucket_name: str, **kwargs):
    utc_minus_1_hr = datetime.utcnow() - timedelta(hours=2)
    filename = utc_minus_1_hr.strftime("%Y-%m-%d-%-H")

    ti = kwargs["ti"]
    ti.xcom_push(key="filename", value=filename)

    filename_with_ext = f"{filename}.json.gz"
    source_url = f"https://data.gharchive.org/{filename_with_ext}"

    logging.info(f"getting file from {source_url}")
    response = requests.get(source_url)

    if response.status_code == 200:
        content = response.content

        from google.cloud import storage

        storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
        storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB

        logging.info("start uploading file content to cloud storage")
        destination_blob_name = f"raw/{filename_with_ext}"
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=destination_blob_name,
            data=content,
            mime_type="application/gzip",
        )
        print("File uploaded to GCS successfully.")
    else:
        raise Exception("Failed to fetch the file from the HTTP source.")
