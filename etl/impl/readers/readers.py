from etl.interfaces import DataInput
import etl.sources as s
from pyspark.sql import DataFrame, SparkSession

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CsvInput(DataInput, s.CSVSourceMixin):

    def read(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Reading csv file from path {self.path} with options {self.options} ")
        df = spark.read.csv(self.path, **self.options)
        return df


class ParquetInput(DataInput, s.ParquetSourceMixin):
    def read(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Reading parquet file from path {self.path} with options {self.options} ")
        df = spark.read.parquet(self.path, **self.options)
        return df

# TODO: S3/Delta/Iceberg input


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
