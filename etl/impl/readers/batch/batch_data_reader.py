from typing import List

from pyspark.sql import SparkSession, DataFrame
from abc import abstractmethod

from etl.interfaces import DataReader

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchDataReader(DataReader):

    def __init__(self, post_read_select_exprs: List[str] = None, post_read_filter_expr: str = None, **kwargs):
        self.post_read_select_exprs = post_read_select_exprs
        self.post_read_filter_expr = post_read_filter_expr

    @abstractmethod
    def read_raw_input(self, spark: SparkSession) -> DataFrame:
        pass

    def read(self, spark: SparkSession) -> DataFrame:
        raw_df = self.read_raw_input(spark)

        post_read_transform_df = self.post_read_select_and_filter(raw_df)

        return post_read_transform_df

    def post_read_select_and_filter(self, df: DataFrame) -> DataFrame:
        logger.info(f"Applying post-read transformations. Select expression = {self.post_read_select_exprs},\n"
                    f" Filter expression = {self.post_read_filter_expr} ")

        applied_df = df.selectExpr(*self.post_read_select_exprs) if self.post_read_select_exprs else df

        return applied_df.where(self.post_read_filter_expr) if self.post_read_filter_expr else applied_df



