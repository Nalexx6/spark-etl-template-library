from etl.interfaces import DataWriter
from pyspark.sql import DataFrame

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConsoleWriter(DataWriter):
    def __init__(self, **kwargs):
        pass

    def write(self, df: DataFrame) -> None:
        df.show(n=100, truncate=False)