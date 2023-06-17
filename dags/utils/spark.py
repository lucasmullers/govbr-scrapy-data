from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


AWS_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
DELTA_PACKAGES = "io.delta:delta-core_2.12:2.3.0,io.delta:delta-storage:2.3.0"


class Spark:
    def __init__(self, access_key: str = "", secret_key: str = "", spark_conf: SparkConf = None):

        if spark_conf is None:
            spark_conf = (
                SparkConf()
                .set("spark.jars.packages", f"{AWS_PACKAGES},{DELTA_PACKAGES}")
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("spark.hadoop.fs.s3a.access.key", access_key)
                .set("spark.hadoop.fs.s3a.secret.key", secret_key)
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            )

        self.spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    def get_spark_session(self):
        return self.spark
