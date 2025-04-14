from etl.interfaces import DataOutput
import etl.sources as s
from pyspark.sql import DataFrame

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CsvOutput(DataOutput, s.CSVSinkMixin):
    def write(self, df: DataFrame) -> None:
        logger.info(f"Writing csv file to path {self.path} with options {self.options}")
        df.write.mode(self.mode).csv(self.path, header=True)


class ParquetOutput(DataOutput, s.ParquetSinkMixin):
    def write(self, df: DataFrame) -> None:
        logger.info(f"Writing parquet file to path {self.path} with options {self.options}")
        df.write.mode(self.mode).parquet(self.path, **self.options)


class KafkaOutput(DataOutput):
    def __init__(self, servers: str, topic: str, options: dict = None):
        self.servers = servers
        self.topic = topic
        self.options = options or {}

    def write(self, df: DataFrame) -> None:
        df.selectExpr("CAST(value AS STRING)") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", self.servers) \
          .option("topic", self.topic) \
          .options(**self.options) \
          .save()
