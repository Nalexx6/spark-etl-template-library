from etl.inputs.input_factory import InputFactory
from etl.readers.batch.batch_data_reader import BatchDataReader
from pyspark.sql import DataFrame, SparkSession

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TODO: S3/Delta/Iceberg input


class S3Reader(BatchDataReader):
    def __init__(self, bucket: str, object_key: str, input_format: str, input_config: dict, input_factory: InputFactory, **kwargs):
        path = f"s3a://{bucket}/{object_key}"
        self.data_input = input_factory.create_input_connector(input_format=input_format, path=path, **input_config)

        super().__init__(**kwargs)

    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        return self.data_input.read(spark)
