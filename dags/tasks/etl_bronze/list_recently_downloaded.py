import logging

from datetime import datetime
from utils.spark import Spark
from pyspark.sql.functions import col

from airflow.hooks.base_hook import BaseHook


def get_recently_downloaded_files(
        ds: str = None,
        conn_id: str = 'aws',
        **kwargs
):
    conn = BaseHook.get_connection(conn_id)

    spark = Spark(access_key=conn.login, secret_key=conn.password).get_spark_session()

    metadata_df = spark.read.format("delta").load('s3a://govbr-data/bronze/metadata/')

    files_to_run_expectations = (
        metadata_df.filter(col("downloaded_at") == datetime.strptime(ds, '%Y-%m-%d'))
        .select("filename")
        .rdd
        .flatMap(lambda x: x)
        .collect()
    )

    logging.info(f"Files to run expectations: {files_to_run_expectations}")

    return files_to_run_expectations
