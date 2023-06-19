from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    S3StoreBackendDefaults
)
from pyspark.sql import DataFrame
from typing import List, Callable

import logging
import yaml


class GreatExpectations:
    def __init__(self, expectations: List[Callable], df: DataFrame, dl_layer: str = "bronze",
                 source: str = "anp", run_test: bool = False) -> None:
        self.expectations = expectations
        self.run_test = run_test
        self.df = df
        self.dl_layer = dl_layer
        self.context = None
        self.source = source

    def run_great_expectations(self) -> dict:
        self.context = self.create_context()
        logging.info(f"Great Expectations context created! {type(self.context)}")

        self.add_data_source_to_context()

        self.suite = self.create_expectations_suite()

        self.append_expectations_to_suite()

        self.add_checkpoint_to_context()

        self.batch_request = self.get_batch_request()

        results = self.run_great_expectations_checkpoint()

        logging.info(f"Great Expectations results:\n{results}")

        if results["success"] is False:
            raise Exception("Great Expectations validation failed")

        return results

    def create_context(self):
        logging.info("Creating Great Expectations context")
        logging.info("Bucket: govbr-data")
        logging.info(f"Layer: {self.dl_layer}")

        store_backend_defaults = S3StoreBackendDefaults(
            default_bucket_name="govbr-data",
            expectations_store_prefix=f'{self.dl_layer}/great_expectations/{self.source}/expectations',
            validations_store_prefix=f'{self.dl_layer}/great_expectations/{self.source}/validations',
            data_docs_prefix=f'{self.dl_layer}/great_expectations/{self.source}/data_docs',
            checkpoint_store_prefix=f'{self.dl_layer}/great_expectations/{self.source}/checkpoints',
            profiler_store_prefix=f'{self.dl_layer}/great_expectations/{self.source}/profiling',
        )

        data_context_config = DataContextConfig(
            store_backend_defaults=store_backend_defaults,
            checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
            data_docs_sites=store_backend_defaults.data_docs_sites,
        )

        return BaseDataContext(project_config=data_context_config)

    def add_data_source_to_context(self):
        data_source_config = {
            "name": "s3_default_data_source",
            "class_name": "Datasource",
            "execution_engine": {"class_name": "SparkDFExecutionEngine"},
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["batch_id"],
                }
            },
        }

        if self.run_test:
            self.context.test_yaml_config(yaml.dump(data_source_config))

        self.context.add_datasource(**data_source_config)

    def create_expectations_suite(self):

        return self.context.create_expectation_suite(
            expectation_suite_name="gx_default", overwrite_existing=True
        )

    def append_expectations_to_suite(self):
        for expectation in self.expectations:
            expectation(self.suite)

        self.context.save_expectation_suite(self.suite, "gx_default_suite", overwrite_existing=True)

    def add_checkpoint_to_context(self):

        checkpoint_config = {
            "name": "default_data_checkpoint",
            "config_version": 1,
            "class_name": "SimpleCheckpoint",
            "expectation_suite_name": "gx_default_suite",
        }

        self.context.add_checkpoint(**checkpoint_config)

    def get_batch_request(self):

        return RuntimeBatchRequest(
            datasource_name="s3_default_data_source",
            data_connector_name="default_runtime_data_connector_name",
            data_asset_name="gx_default_asset",
            runtime_parameters={"batch_data": self.df},
            batch_identifiers={"batch_id": "default_identifier"},
        )

    def run_great_expectations_checkpoint(self):
        return self.context.run_checkpoint(
            checkpoint_name="default_data_checkpoint",
            validations=[
                {"batch_request": self.batch_request},
            ],
        )
