from etl.interfaces import DataWriter
from pyspark.sql import DataFrame

import etl.utils.schema_utils as su
from etl.writers.writer_factory import create_writer

import logging

from etl.metadata.pipeline_schema import OutputConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamDataWriter(DataWriter):

    def __init__(self, writer_format: str, writer_config: dict, output: OutputConfig = None):

        if writer_config.get("for_each_batch"):
            logger.info(f"foreachBatch option enabled. Loading batch writer for {writer_format} format")
            self.batch_writer = create_writer(writer_format, writer_config, output)
        else:
            self.batch_writer = None
            if writer_format == "kafka":
                self.encoder_func = su.encode_value(output.format, output.config.get("schema_filepath"))
            else:
                self.encoder_func = None

        self.writer_format = writer_format
        self.checkpoint_location = writer_config.get("checkpoint_location")
        # (?) TODO: options validation for each format
        self.writer_config = writer_config.get("options") if writer_config.__contains__("options") else {}

    def write(self, df: DataFrame) -> None:
        if self.batch_writer:
            self.microbatch_write(df)
        else:
            self.native_write(df)

    def native_write(self, df: DataFrame) -> None:

        encoded_df = df.transform(self.encoder_func) if self.encoder_func else df

        (encoded_df.writeStream
         .format(self.writer_format)
         .option("checkpointLocation", self.checkpoint_location)
         .options(**self.writer_config)
         .trigger(once=True)
         .start()
         .awaitTermination())

    def microbatch_write(self, df: DataFrame) -> None:
        (df.writeStream
         .foreachBatch(self.batch_writer)
         .option("checkpointLocation", self.checkpoint_location)
         .trigger(once=True)
         .start()
         .awaitTermination())
