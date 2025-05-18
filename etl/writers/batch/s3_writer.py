from pyspark.sql import DataFrame
from etl.interfaces import DataWriter

from etl.outputs.output_factory import OutputFactory

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Writer(DataWriter):

    def __init__(self, bucket: str, object_key: str, output_format: str, output_config: dict,
                 output_factory: OutputFactory, **kwargs):
        path = f"s3a://{bucket}/{object_key}"
        self.data_output = output_factory.create_output_connector(output_format=output_format, path=path,
                                                                           **output_config)

    def write(self, df: DataFrame) -> None:
        return self.data_output.write(df)