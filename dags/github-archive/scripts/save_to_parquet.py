from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


def json_to_parquet(bucket_name: str, **kwargs):
    service_account = "/opt/airflow/config/service_account.json"
    conf = (
        SparkConf()
        .setMaster("spark://spark:7077")
        .setAppName("reading-json")
        .set("spark.jars", "/opt/airflow/plugins/gcs-connector-hadoop3-latest.jar")
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .set(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            service_account,
        )
    )

    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
    hadoop_conf.set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", service_account)

    spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

    ti = kwargs["ti"]
    filename = ti.xcom_pull(key="filename")
    filepath = f"gs://{bucket_name}/raw/{filename}.json.gz"

    print(f"Reading json from {filepath}")
    schema = StructType(
        [
            StructField("type", StringType(), True),
            StructField("created_at", TimestampType(), True),
        ]
    )
    df = spark.read.json(filepath, schema=schema)

    dest_filepath = f"gs://{bucket_name}/parquet/{filename}/"

    print(f"Wrting parquet to {dest_filepath} in cloud storage")
    df.repartition(2).write.parquet(dest_filepath, mode="overwrite")

    print("Writing parquet files successfully")
