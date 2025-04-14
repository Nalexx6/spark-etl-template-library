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


class GenericTransformer:

    # props: dict(str, str),
    # get col names, operations from props
    # general props structure
    pass