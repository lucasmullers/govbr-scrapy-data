import json
import logging

from datetime import datetime
from utils.spark import Spark
from delta.tables import DeltaTable
from airflow.hooks.base_hook import BaseHook
from utils.great_expectations import GreatExpectations
from great_expectations.core.expectation_configuration import ExpectationConfiguration

from pyspark.sql.functions import regexp_replace, when, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, DateType


def expectation_to_check_categorical_cols_values(suite):
    cols_and_checks = {
        "Produto": ["DIESEL S10", "DIESEL S50", "DIESEL", "ETANOL", "GNV", "GASOLINA", "GASOLINA ADITIVADA"],
        "Regiao - Sigla": ["NE", "N", "S", "SE", "CO"],
        "Estado - Sigla": [
            "SC",
            "RO",
            "PI",
            "AM",
            "RR",
            "GO",
            "TO",
            "MT",
            "SP",
            "ES",
            "PB",
            "RS",
            "MS",
            "AL",
            "MG",
            "PA",
            "BA",
            "SE",
            "PE",
            "CE",
            "RN",
            "RJ",
            "MA",
            "AC",
            "DF",
            "PR",
            "AP",
        ],
    }

    for column in cols_and_checks:
        expectation_configuration = ExpectationConfiguration(
            expectation_type="expect_column_distinct_values_to_be_in_set",
            kwargs={"column": column, "value_set": cols_and_checks[column]},
        )

        suite.add_expectation(expectation_configuration=expectation_configuration)


def expectation_to_check_regex_values(suite):
    cols_and_checks = {
        "CNPJ da Revenda": r"\d{2}.\d{3}.\d{3}\/\d{4}-\d{2}",
        "Cep": r"\d{5}-\d{3}"
    }

    for column in cols_and_checks:
        expectation_configuration = ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={"column": column, "regex": cols_and_checks[column]},
        )
        suite.add_expectation(expectation_configuration=expectation_configuration)


def update_metadata(
        spark,
        filename: str = "",
        downloaded_at: datetime = datetime(1970, 1, 1),
        validation_result: bool = False,
        validation_message: str = "") -> None:

    metadata = [
        {
            "downloaded_at": downloaded_at,
            "filename": filename,
            "ge_validation_result": validation_result,
            "ge_validation_message": validation_message,
        }
    ]
    rdd = spark.sparkContext.parallelize(metadata)

    schema = StructType([
        StructField("downloaded_at", TimestampType(), True),
        StructField("filename", StringType(), True),
        StructField("ge_validation_result", StringType(), True),
        StructField("ge_validation_message", StringType(), True)
    ])
    metadata_df = spark.createDataFrame(rdd, schema)
    logging.info(f"{metadata_df.show(1, truncate=False)}")

    delta_table = DeltaTable.forPath(spark, "s3a://govbr-data/bronze/metadata/")
    (
        delta_table.alias("old")
        .merge(metadata_df.alias("new"), "old.filename = new.filename AND old.downloaded_at = new.downloaded_at")
        .whenMatchedUpdate(set={
            "ge_validation_result": "new.ge_validation_result",
            "ge_validation_message": "new.ge_validation_message",
        })
        .execute()
    )


def run_great_expectations_anp_data(
    filename: str = "ca-2017-02.csv",
    conn_id: str = 'aws',
    ds=None,
    **kwargs
):
    logging.info(f"Running great expectations for file: {filename}")

    conn = BaseHook.get_connection(conn_id)

    spark = Spark(access_key=conn.login, secret_key=conn.password).get_spark_session()

    schema = StructType([
        StructField("Regiao - Sigla", StringType(), True),
        StructField("Estado - Sigla", StringType(), True),
        StructField("Municipio", StringType(), True),
        StructField("Revenda", StringType(), True),
        StructField("CNPJ da Revenda", StringType(), True),
        StructField("Nome da Rua", StringType(), True),
        StructField("Numero Rua", StringType(), True),
        StructField("Complemento", StringType(), True),
        StructField("Bairro", StringType(), True),
        StructField("Cep", StringType(), True),
        StructField("Produto", StringType(), True),
        StructField("Data da Coleta", DateType(), True),
        StructField("Valor de Venda", StringType(), True),
        StructField("Valor de Compra", StringType(), True),
        StructField("Unidade de Medida", StringType(), True),
        StructField("Bandeira", StringType(), True),
    ])

    df = (
        spark.read.csv(f"s3a://govbr-data/bronze/ANP/{filename}", header=True, sep=";", timestampFormat="dd/MM/yyyy",
                       schema=schema)
        .withColumn("Valor de Compra", regexp_replace("Valor de Compra", ",", ".").cast(FloatType()))
        .withColumn("Valor de Venda", regexp_replace("Valor de Venda", ",", ".").cast(FloatType()))
        .withColumn("Produto", when(col("Produto") == "ETA", "ETANOL")
                    .when(col("Produto") == "GASOL", "GASOLINA")
                    .otherwise(col("Produto")))
    )

    logging.info(f"Sample of file: {df.limit(5).show(5, truncate=False)}")

    results = GreatExpectations(
        expectations=[expectation_to_check_categorical_cols_values, expectation_to_check_regex_values],
        df=df,
        dl_layer="bronze",
        source="ANP",
        run_test=True,
    ).run_great_expectations()

    execution_date = datetime.strptime(ds, "%Y-%m-%d")

    update_metadata(spark=spark, filename=filename, downloaded_at=execution_date, validation_result=results["success"],
                    validation_message=json.dumps(results.to_json_dict()))
