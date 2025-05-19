from abc import ABC, abstractmethod
from typing import Callable

from pyspark.sql import DataFrame, SparkSession

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, df: DataFrame, spark: SparkSession = None) -> DataFrame:
        """Apply transformations to the input DataFrame"""
        pass


class DataReader(ABC):
    def __init__(self, flatten_func: Callable[[DataFrame], DataFrame] = None, post_read_select_exprs: list[str] = None,
                 post_read_filter_expr: str = None, **kwargs):

        self.flatten_func = flatten_func
        self.post_read_select_exprs = post_read_select_exprs
        self.post_read_filter_expr = post_read_filter_expr

    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        """
        Read raw data using the configured reader, apply post-read transformations to it, if needed.
        Return a Spark DataFrame
        """

        pass

    def post_read_select_and_filter(self, df: DataFrame) -> DataFrame:
        logger.info(f"Applying post-read transformations. Select expression = {self.post_read_select_exprs},"
                    f" Filter expression = {self.post_read_filter_expr} ")

        applied_df = df.selectExpr(*self.post_read_select_exprs) if self.post_read_select_exprs else df
        return applied_df.where(self.post_read_filter_expr) if self.post_read_filter_expr else applied_df

    def post_read_flatten(self, df: DataFrame) -> DataFrame:
        flattened_df = self.flatten_func(df) if self.flatten_func else df

        return flattened_df


class DataWriter(ABC):

    def __call__(self, df: DataFrame, epoch_id) -> None:
        self.write(df)

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass


class DataInput(ABC):
    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        """Read raw data from configured input source and return a Spark DataFrame"""
        pass


class DataOutput(ABC):
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write the processed DataFrame to a destination"""
        pass
