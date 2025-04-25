from etl.interfaces import DataOutput, DataWriter
from etl.impl.outputs.outputs_factory import create_output_connector
import etl.sources as s
from pyspark.sql import DataFrame

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConsoleWriter(DataWriter):
    def __init__(self, **kwargs):
        pass

    def write(self, df: DataFrame) -> None:
        df.show(truncate=False)


class S3Writer(DataWriter):

    def __init__(self, bucket: str, object_key: str, output_format: str, output_config: dict):
        path = f"s3a://{bucket}/{object_key}"
        logger.info(f"Initializing {output_format} output connector with {path} path")
        self.data_output = create_output_connector(output_format=output_format, path=path, **output_config)

    def write(self, df: DataFrame) -> None:
        return self.data_output.write(df)


class HdfsWriter(DataWriter):
    def __init__(self, server_url: str, path: str, output_format: str, output_config: dict):
        path = f"hdfs://{server_url}/{path}"
        logger.info(f"Initializing {output_format} output connector with {path} path")
        self.data_output = create_output_connector(output_format=output_format, path=path, **output_config)

    def write(self, df: DataFrame) -> None:
        return self.data_output.write(df)