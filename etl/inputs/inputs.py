from etl.interfaces import DataInput
import etl.sources as s
from pyspark.sql import DataFrame, SparkSession

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CsvInput(DataInput, s.CSVSourceMixin):

    def __init__(self, path: str, header: bool = True, infer_schema: bool = True, **kwargs):
        base_options = {
            "header": header,
            "inferSchema": infer_schema,
        }
        if kwargs:
            base_options.update(kwargs)
        super().__init__(path, options=base_options)

    def read(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Reading csv file from path {self.path} with options {self.options} ")
        df = spark.read.csv(self.path, **self.options)
        return df


class ParquetInput(DataInput, s.FileSourceMixin):
    def read(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Reading parquet file from path {self.path} with options {self.options} ")
        df = spark.read.parquet(self.path, **self.options)
        return df


class JsonInput(DataInput, s.FileSourceMixin):
    def read(self, spark: SparkSession) -> DataFrame:
        logger.info(f"Reading json file from path {self.path} with options {self.options} ")
        df = spark.read.json(self.path, **self.options)
        return df


