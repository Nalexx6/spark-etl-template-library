from etl.impl.readers.stream.stream_data_reader import StreamDataReader
from pyspark.sql import DataFrame, SparkSession

from pyspark.sql import functions as f

import etl.utils.schema_utils as su
from etl.impl.inputs.inputs_factory import create_input_connector

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# TODO: S3/Delta/Iceberg input


class KafkaReader(StreamDataReader):

    def __init__(self, servers: list[str], topics: list[str], input_format: str, input_config: dict,
                 starting_offsets="earliest",
                 options: dict = None, **kwargs):
        self.servers = servers
        self.topics = topics
        self.starting_offsets = starting_offsets
        self.options = options or {}

        schema_filepath = input_config.get("schema_filepath")

        self.source_schema = su.load_avro_schema(schema_filepath) \
            if input_format == "avro" \
            else su.load_json_schema(schema_filepath)
        self.flatten_func = su.decode_avro_value if input_format == "avro" else su.decode_json_value

        super().__init__(**kwargs)

    def read_stream(self, spark: SparkSession) -> DataFrame:
        return spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", ','.join(self.servers)) \
            .option("subscribe", "|".join(self.topics)) \
            .option("startingOffsets", self.starting_offsets) \
            .options(**self.options) \
            .load()

    def post_read_flatten(self, df: DataFrame) -> DataFrame:
        flattened_df = self.flatten_func(df, self.source_schema)

        return flattened_df
