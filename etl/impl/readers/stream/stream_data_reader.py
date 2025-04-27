from pyspark.sql import SparkSession, DataFrame
from abc import abstractmethod

from etl.impl.readers.reader_interface import DataReader

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamDataReader(DataReader):

    def __init__(self, flatten: bool = True,  post_read_select_exprs: list[str] = None,
                 post_read_filter_expr: str = None, **kwargs):

        self.flatten = flatten

        super().__init__(post_read_select_exprs, post_read_filter_expr, **kwargs)

    @abstractmethod
    def read_stream(self, spark: SparkSession) -> DataFrame:
        pass

    def read(self, spark: SparkSession) -> DataFrame:
        raw_df = self.read_stream(spark)

        post_read_flatten_df = self.post_read_flatten(raw_df) if self.flatten else raw_df
        post_read_transform_df = self.post_read_select_and_filter(post_read_flatten_df)

        return post_read_transform_df

    @abstractmethod
    def post_read_flatten(self, df: DataFrame) -> DataFrame:
        pass
