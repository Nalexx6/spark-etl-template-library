from pyspark.sql import DataFrame
from etl.interfaces import DataWriter

import etl.utils.schema_utils as su

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaWriter(DataWriter):

    def __init__(self, servers: list[str], topic: str, output_format: str, output_config: dict,
                 options: dict = None, **kwargs):
        self.servers = servers
        self.topic = topic
        self.options = options or {}

        self.encoder_func = su.encode_value(output_format, output_config.get("schema_filepath"))

    def write(self, df: DataFrame) -> None:
        encoded_df = df.transform(self.encoder_func)

        encoded_df.show(truncate=False)

        (encoded_df.withColumn("topic", f.lit(self.topic))
         .write
         .format("kafka")
         .option("kafka.bootstrap.servers", ','.join(self.servers))
         .options(**self.options)
         .save())