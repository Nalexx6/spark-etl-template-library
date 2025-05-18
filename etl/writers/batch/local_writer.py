from etl.interfaces import DataWriter
from pyspark.sql import DataFrame

import logging

from etl.outputs.output_factory import OutputFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LocalWriter(DataWriter):
    def __init__(self, path: str, output_format: str, output_config: dict,
                 output_factory: OutputFactory, **kwargs):
        self.data_output = output_factory.create_output_connector(output_format=output_format, path=path,
                                                                           **output_config)

    def write(self, df: DataFrame) -> None:
        return self.data_output.write(df)