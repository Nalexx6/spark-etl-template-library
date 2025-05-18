from pyspark.sql import DataFrame, SparkSession
from etl.readers.batch.batch_data_reader import BatchDataReader

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RedshiftReader(BatchDataReader):
    def __init__(self, url: str, table: str, options: dict, **kwargs):
        self.url = url
        self.table = table
        self.options = options
        super().__init__(**kwargs)

    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        return spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", self.table) \
            .options(**self.options) \
            .load()
