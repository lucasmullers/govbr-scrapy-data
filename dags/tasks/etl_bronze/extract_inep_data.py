import logging

from os import getenv, remove
from shutil import rmtree
from requests import get
from datetime import datetime
from dotenv import load_dotenv
from zipfile import ZipFile
from io import BytesIO

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

load_dotenv()


class INEPDataExtractor:
    def __init__(self) -> None:
        AWS_PACKAGES = "org.apache.hadoop:hadoop-aws:3.0.3,com.amazonaws:aws-java-sdk-bundle:1.11.271"
        self.spark = (
            SparkSession
            .builder
            .config("spark.jars.packages", f"{AWS_PACKAGES}")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", "AKIASJG5OPVC5WBZYNFY")
            .config("spark.hadoop.fs.s3a.secret.key", "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")
            .getOrCreate()
        )

    def _check_if_file_already_downloaded(self, url: str, display_name: str) -> bool:
        try:
            metadata_df = self.spark.read.format("parquet").load("s3a://govbr-data/bronze/metadata/")
            print(metadata_df.show(10, truncate=False))
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
        columns = ["url", "display_name", "source", "downloaded_at", "filename", "datasource"]
        metadata_df = self.spark.createDataFrame([(url, display_name, "inep", datetime.now(), filename, datasource)],
                                                 columns)
        logging.info(f"{metadata_df.show(1, truncate=False)}")

        try:


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
            csv_files_on_zip = [f"./data.zip/{file}" for file in zip_file.namelist() if ".csv" in file.lower()]
            logging.info(f"CSV files on zip: {csv_files_on_zip}")

            for file in csv_files_on_zip:
                self._update_metadata(url=url,
                                      display_name=display_name,
                                      filename=file.split("/")[-1],
                                      datasource=datasource)
                s3_hook.load_file(file, f"bronze/inep/{datasource}/{file.split('/')[-1]}", "govbr-data")

            rmtree("./data.zip", ignore_errors=True)
        else:
            raise (f"File extension not supported! The full url is: {url}")
