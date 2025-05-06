from etl.interfaces import DataTransformer
from pyspark.sql import DataFrame, functions as f, Window, SparkSession

from etl.impl.inputs.inputs_factory import create_input_connector

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaxiMetadataPreprocessTransformer(DataTransformer):

    def transform(self, df: DataFrame, spark: SparkSession = None) -> DataFrame:
        df_enriched = (df.withColumn('pickup_day_of_week', f.dayofweek(df['pickup_datetime']))
                       .withColumn('pickup_day', f.dayofmonth(df['pickup_datetime']))
                       .withColumn('pickup_hour', f.hour(df['pickup_datetime']))
                       .withColumn('dropoff_day_of_week', f.dayofweek(df['dropoff_datetime']))
                       .withColumn('dropoff_day', f.dayofmonth(df['dropoff_datetime']))
                       .withColumn('dropoff_hour', f.hour(df['dropoff_datetime'])))

        return df_enriched


class TaxiMetadataAggTransformer(DataTransformer):

    def __init__(self, bucket: str, licenses_metadata_path: str, zones_metadata_path: str):
        self.licenses_metadata_path = f"s3a://{bucket}/{licenses_metadata_path}"
        self.zones_metadata_path = f"s3a://{bucket}/{zones_metadata_path}"

    def transform(self, df: DataFrame, spark: SparkSession = None) -> DataFrame:
        df = df.cache()

        licenses, zones = self.get_trips_metadata(spark)

        pickup_df = df.groupBy("Hvfhs_license_num", "PULocationID", "pickup_hour") \
            .agg(f.count("*").alias("pickup_count")) \
            .withColumnRenamed("PULocationID", "LocationID") \
            .withColumnRenamed("pickup_hour", "hour")

        dropoff_df = df.groupBy("Hvfhs_license_num", "DOLocationID", "dropoff_hour") \
            .agg(f.count("*").alias("dropoff_count")) \
            .withColumnRenamed("DOLocationID", "LocationID") \
            .withColumnRenamed("dropoff_hour", "hour")

        trips_by_hour_license = df.groupBy("Hvfhs_license_num", "dropoff_hour").count()

        window_spec = Window.partitionBy("dropoff_hour").orderBy(f.desc("count"))
        top_licenses_df = trips_by_hour_license \
            .withColumn("rank", f.row_number().over(window_spec))

        result = (pickup_df.alias("p")
                  .join(dropoff_df.alias("d"),
                        on=["Hvfhs_license_num", "LocationID", "hour"],
                        how="outer")
                  .join(self.calculate_tips_metrics(df), on=["Hvfhs_license_num", "LocationID", "hour"], how="outer")
                  .join(top_licenses_df, on="Hvfhs_license_num", how="left")
                  .join(licenses, on="Hvfhs_license_num", how="left")
                  .join(zones, on="LocationID", how="left"))

        return result


    def calculate_tips_metrics(self, df: DataFrame) -> DataFrame:
        bucketed_df = (df.withColumnRenamed("PULocationID", "LocationID")
                       .withColumnRenamed("pickup_hour", "hour")
                       .withColumn("distance_bucket", f.floor(f.col("trip_miles")))  # 0-1, 1-2, etc.
                       .withColumn("duration_bucket", (f.floor(f.col("trip_time") / 300) * 5))  # 5-min bins (300s)
                       .withColumn("trip_speed", (f.col("trip_miles") / f.col("trip_time")) * 3600)
                       .withColumn("speed_bucket", (f.floor(f.col("trip_speed") / 5) * 5)))  # 5 mph bins

        avg_tip_by_distance = bucketed_df.groupBy("Hvfhs_license_num", "LocationID", "hour", "distance_bucket") \
            .agg(
            f.avg("tips").alias("avg_tips"),
        ).withColumnRenamed("distance_bucket", "bucket_name")

        # Tip by duration bucket
        avg_tip_by_duration = bucketed_df.groupBy("Hvfhs_license_num", "LocationID", "hour", "duration_bucket") \
            .agg(
            f.avg("tips").alias("avg_tips"),
        ).withColumnRenamed("duration_bucket", "bucket_name")

        # Tip by speed bucket
        avg_tip_by_speed = bucketed_df.groupBy("Hvfhs_license_num", "LocationID", "hour", "speed_bucket") \
            .agg(
            f.avg("tips").alias("avg_tips"),
        ).withColumnRenamed("speed_bucket", "bucket_name")

        unified_buckets = (
            avg_tip_by_distance.select("Hvfhs_license_num", "LocationID", "hour", "bucket_name", "avg_tips")
            .unionByName(
                avg_tip_by_duration.select("Hvfhs_license_num", "LocationID", "hour", "bucket_name", "avg_tips"))
            .unionByName(
                avg_tip_by_speed.select("Hvfhs_license_num", "LocationID", "hour", "bucket_name", "avg_tips"))
        )

        return unified_buckets

    def get_trips_metadata(self, spark: SparkSession) -> (DataFrame, DataFrame):
        licenses_df = create_input_connector("csv", path=self.licenses_metadata_path).read(spark)
        zones_df = create_input_connector("csv", path=self.zones_metadata_path).read(spark)

        return licenses_df, zones_df
