from etl.interfaces import DataTransformer
from pyspark.sql import DataFrame

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NoOpTransformer(DataTransformer):
    def transform(self, df: DataFrame) -> DataFrame:
        return df  # No transformation