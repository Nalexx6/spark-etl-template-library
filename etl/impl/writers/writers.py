from etl.interfaces import DataWriter
from etl.impl.outputs.outputs_factory import create_output_connector
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


import etl.utils.schema_utils as su

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConsoleWriter(DataWriter):
    def __init__(self, **kwargs):
        pass

    def write(self, df: DataFrame) -> None:
        df.show(truncate=False)


class S3Writer(DataWriter):

    def __init__(self, bucket: str, object_key: str, output_format: str, output_config: dict, **kwargs):
        path = f"s3a://{bucket}/{object_key}"
        logger.info(f"Initializing {output_format} output connector with {path} path")
        self.data_output = create_output_connector(output_format=output_format, path=path, **output_config)

    def write(self, df: DataFrame) -> None:
        return self.data_output.write(df)


class HdfsWriter(DataWriter):
    def __init__(self, server_url: str, path: str, output_format: str, output_config: dict, **kwargs):
        path = f"hdfs://{server_url}/{path}"
        logger.info(f"Initializing {output_format} output connector with {path} path")
        self.data_output = create_output_connector(output_format=output_format, path=path, **output_config)

    def write(self, df: DataFrame) -> None:
        return self.data_output.write(df)


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
