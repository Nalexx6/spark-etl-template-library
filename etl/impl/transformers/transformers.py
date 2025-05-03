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
        # df_tr.show()

        return df_tr


class GroupByFirstTransformer(DataTransformer):

    def __init__(self, grouping_key: str, agg_column: str):
        self.grouping_key = grouping_key
        self.agg_column = agg_column

    def transform(self, df: DataFrame) -> DataFrame:
        df_tr = df.groupby(self.grouping_key).agg(f.first(col=self.agg_column).alias(self.agg_column))

        return df_tr


class TaxiMetadataTransformer(DataTransformer):

    def transform(self, df: DataFrame) -> DataFrame:
        df_enriched = (df.withColumn('pickup_day_of_week', f.dayofweek(df['pickup_datetime']))
                         .withColumn('pickup_day', f.dayofmonth(df['pickup_datetime']))
                         .withColumn('pickup_hour', f.hour(df['pickup_datetime']))
                         .withColumn('dropoff_day_of_week', f.dayofweek(df['dropoff_datetime']))
                         .withColumn('dropoff_day', f.dayofmonth(df['dropoff_datetime']))
                         .withColumn('dropoff_hour', f.hour(df['dropoff_datetime'])))

        return df_enriched
    

