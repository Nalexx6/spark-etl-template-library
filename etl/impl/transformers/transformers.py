from etl.interfaces import DataTransformer
from pyspark.sql import DataFrame, functions as f

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NoOpTransformer(DataTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df  # No transformation


class TimestampTransformer(DataTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        df_tr = df.withColumn("cur_timestamp", f.current_timestamp())
        df_tr.show()

        return df_tr


class DateTransformer(DataTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        df_tr = df.withColumn("cur_date", f.current_date())
        df_tr.show()

        return df_tr


class GroupByFirstTransformer(DataTransformer):

    def __init__(self, grouping_key: str, agg_column: str):
        self.grouping_key = grouping_key
        self.agg_column = agg_column

    def transform(self, df: DataFrame) -> DataFrame:
        df_tr = df.groupby(self.grouping_key).agg(f.first(col=self.agg_column).alias(self.agg_column))

        return df_tr


class GenericTransformer:

    # props: dict(str, str),
    # get col names, operations from props
    # general props structure
    pass
