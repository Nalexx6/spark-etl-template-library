from etl.interfaces import DataInput, DataReader
from pyspark.sql import DataFrame, SparkSession


from etl.impl.inputs.inputs_factory import create_input_connector

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TODO: S3/Delta/Iceberg input


class S3Reader(DataReader):
    def __init__(self, input_format: str, bucket: str, object_key: str, **kwargs):
        path = f"s3a://{bucket}/{object_key}"
        logger.info(f"Initializing {input_format} input connector with {path} path")
        self.data_input = create_input_connector(input_format=input_format, path=path, **kwargs)

    def read(self, spark) -> DataFrame:
        return self.data_input.read(spark)


class KafkaInput(DataInput):
    def __init__(self, servers: str, topic: str, starting_offsets="earliest", options: dict = None):
        self.servers = servers
        self.topic = topic
        self.starting_offsets = starting_offsets
        self.options = options or {}

    def read(self, spark: SparkSession) -> DataFrame:
        return spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", self.starting_offsets) \
            .options(**self.options) \
            .load()
