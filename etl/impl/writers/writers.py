from etl.interfaces import DataOutput, DataWriter
from etl.impl.outputs.outputs_factory import create_output_connector
import etl.sources as s
from pyspark.sql import DataFrame

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Writer(DataWriter):

    def __init__(self, output_format: str, bucket: str, object_key: str, **kwargs):
        path = f"s3a://{bucket}/{object_key}"
        logger.info(f"Initializing {output_format} output connector with {path} path")
        self.data_output = create_output_connector(output_format=output_format, path=path, **kwargs)

    def write(self, df: DataFrame) -> None:
        return self.data_output.write(df)
