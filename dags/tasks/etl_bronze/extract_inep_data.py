import logging

from os import getenv, remove
from shutil import rmtree
from requests import get
from datetime import datetime
from dotenv import load_dotenv
from zipfile import ZipFile
from io import BytesIO

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

load_dotenv()


class INEPDataExtractor:
    def __init__(self) -> None:
        AWS_PACKAGES = "org.apache.hadoop:hadoop-aws:3.2.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026"
        DELTA_PACKAGES = "io.delta:delta-core_2.12:2.0.0,io.delta:delta-storage:2.0.0"

        self.spark = (
            SparkSession
            .builder
            .config("spark.jars.packages", f"{AWS_PACKAGES},{DELTA_PACKAGES}")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", getenv("access_key"))
            .config("spark.hadoop.fs.s3a.secret.key", getenv("secret_key"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .getOrCreate()
        )

    def _check_if_file_already_downloaded(self, url: str, display_name: str) -> bool:
        try:
            metadata_df = self.spark.read.format("parquet").load("s3a://govbr-data/bronze/metadata/")

            already_downloaded_files = (
                metadata_df.filter(col("url") == url)
                .select("display_name")
                .rdd.flatMap(lambda x: x).collect()
            )
            logging.info(f"Already downloaded files: {already_downloaded_files}")

            if display_name in already_downloaded_files:
                return True
        except:
            logging.info("File not found in metadata.")
            return False

    def _update_metadata(
            self,
            url: str = "",
            display_name: str = "",
            filename: str = "",
            datasource: str = "") -> None:
        metadata = [{"url": url, "display_name": display_name, "source": "inep", "downloaded_at": datetime.now(),
                     "filename": filename, "datasource": datasource}]

        rdd = self.spark.sparkContext.parallelize(metadata)
        schema = StructType([
            StructField("url", StringType(), True),
            StructField("display_name", StringType(), True),
            StructField("source", StringType(), True),
            StructField("downloaded_at", TimestampType(), True),
            StructField("filename", StringType(), True),
            StructField("datasource", StringType(), True)
        ])
        metadata_df = self.spark.createDataFrame(rdd, schema)
        logging.info(f"{metadata_df.show(1, truncate=False)}")

        try:
            delta_table = DeltaTable.forPath(self.spark, "s3a://govbr-data/bronze/metadata/")
            (
                delta_table.alias("oldData")
                .merge(metadata_df.alias("newData"), "oldData.url = newData.url")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
            )
        except:
            metadata_df.write.format("delta").mode("append").save("s3a://govbr-data/bronze/metadata/")

    def upload_file_to_s3(self,
                          url: str,
                          display_name: str,
                          datasource: str = "enem",
                          conn_id: str = "aws",
                          **kwargs) -> None:

        logging.info(f"Downloading file from {url} and displayed as {display_name}")

        if self._check_if_file_already_downloaded(url=url, display_name=display_name):
            return

        s3_hook = S3Hook(conn_id)

        response = get(url, allow_redirects=True, verify=False, stream=True)

        if response.status_code != 200:
            raise Exception(f"Error downloading file from {url}! Status code: {response.status_code}")

        if ".csv" in url:
            filename = url.split("/")[-1]
            with open(f"./{filename}", "wb") as f:
                f.write(response.content)
            s3_hook.load_file(f"./{filename}", f"bronze/inep/{datasource}/{filename}", "govbr-data")
            remove(f"./{filename}")

            self._update_metadata(url=url,
                                  display_name=display_name,
                                  filename=filename.split("/")[-1],
                                  datasource=datasource)
        elif ".zip" in url:
            zip_file = ZipFile(BytesIO(response.content))
            zip_file.extractall("./data.zip")
            print(zip_file.namelist())
            files_on_zip = [f"./data.zip/{file}" for file in zip_file.namelist() if file.lower().split(".")[-1] in {"csv", "txt"}]
            logging.info(f"csv/txt files on zip: {files_on_zip}")

            for file in files_on_zip:
                s3_hook.load_file(file, f"bronze/inep/{datasource}/{file.split('/')[-1]}", "govbr-data")
                self._update_metadata(url=url,
                                      display_name=display_name,
                                      filename=file.split("/")[-1],
                                      datasource=datasource)

            rmtree("./data.zip", ignore_errors=True)
        else:
            raise (f"File extension not supported! The full url is: {url}")
