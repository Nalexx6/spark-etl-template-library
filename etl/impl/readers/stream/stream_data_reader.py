from pyspark.sql import SparkSession, DataFrame
from abc import abstractmethod

import etl.utils.schema_utils as su
from etl.impl.readers.reader_interface import DataReader

import logging

from etl.metadata.pipeline_schema import InputConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StreamDataReader(DataReader):

    def __init__(self, reader_format: str, options: dict = None, input: InputConfig = None, **kwargs):
        self.source_format = reader_format
        self.options = options or {}

        if reader_format == 'kafka':
            flatten_func = su.decode_value(input.format, input.config.get("schema_filepath"))
        else:
            flatten_func = None

        super().__init__(flatten_func=flatten_func, **kwargs)

    def read(self, spark: SparkSession) -> DataFrame:
        print(self.options)
        # TODO: refactor
        raw_df = (spark.readStream
                .format(self.source_format)
                .options(**self.options)
                .load())

        post_read_flatten_df = self.post_read_flatten(raw_df)
        post_read_transform_df = self.post_read_select_and_filter(post_read_flatten_df)

        return post_read_transform_df

