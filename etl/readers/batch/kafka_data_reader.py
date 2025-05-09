from etl.readers.batch.batch_data_reader import BatchDataReader
from pyspark.sql import DataFrame, SparkSession

import etl.utils.schema_utils as su


import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# TODO: S3/Delta/Iceberg input

class KafkaReader(BatchDataReader):

    def __init__(self, servers: list[str], topics: list[str], input_format: str, input_config: dict,
                 starting_offsets="earliest",
                 options: dict = None, **kwargs):
        self.servers = servers
        self.topics = topics
        self.starting_offsets = starting_offsets
        self.options = options or {}

        flatten_func = su.decode_value(input_format, input_config.get("schema_filepath"))

        super().__init__(flatten_func=flatten_func, **kwargs)

    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        return spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ','.join(self.servers)) \
            .option("subscribe", "|".join(self.topics)) \
            .option("startingOffsets", self.starting_offsets) \
            .options(**self.options) \
            .load()
