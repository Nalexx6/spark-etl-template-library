from pyspark.sql import SparkSession, DataFrame
from abc import abstractmethod

from etl.interfaces import DataReader

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchDataReader(DataReader):

    @abstractmethod
    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        pass

    def read(self, spark: SparkSession) -> DataFrame:
        raw_df = self.read_raw_input(spark)

        post_read_flatten_df = self.post_read_flatten(raw_df)
        post_read_transform_df = self.post_read_select_and_filter(post_read_flatten_df)

        return post_read_transform_df
