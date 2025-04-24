from etl.interfaces import DataInput, DataReader
from etl.impl.readers.batch.batch_data_reader import BatchDataReader
from pyspark.sql import DataFrame, SparkSession


from etl.impl.inputs.inputs_factory import create_input_connector

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TODO: S3/Delta/Iceberg input


class S3Reader(BatchDataReader):
    def __init__(self, input_format: str, bucket: str, object_key: str, **kwargs):
        path = f"s3a://{bucket}/{object_key}"
        logger.info(f"Initializing {input_format} input connector with {path} path")
        self.data_input = create_input_connector(input_format=input_format, path=path, **kwargs)

        super().__init__(**kwargs)

    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        return self.data_input.read(spark)
