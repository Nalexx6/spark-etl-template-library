from etl.interfaces import DataReader
from pyspark.sql import DataFrame, SparkSession

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# TODO: S3/Delta/Iceberg input

class KafkaReader(DataReader):
    def __init__(self, servers: list[str], topics: list[str], starting_offsets="earliest",
                 options: dict = None, **kwargs):
        self.servers = servers
        self.topics = topics
        self.starting_offsets = starting_offsets
        self.options = options or {}

    def read(self, spark: SparkSession) -> DataFrame:
        return spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ','.join(self.servers)) \
            .option("subscribe", "|".join(self.topics)) \
            .option("startingOffsets", self.starting_offsets) \
            .options(**self.options) \
            .load()
