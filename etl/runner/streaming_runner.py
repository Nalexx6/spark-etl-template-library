from typing import List

from etl.impl.readers.stream.stream_data_reader import StreamDataReader
from etl.impl.transformers.transformers_factory import create_transformers
from etl.impl.writers.stream_data_writer import StreamDataWriter
from etl.interfaces import DataTransformer, DataWriter
from pyspark.sql import SparkSession

from etl.metadata.pipeline_schema import PipelineMetadata


class StreamPipelineRunner:
    def __init__(self, spark: SparkSession, metadata: PipelineMetadata):
        self.spark = spark

        self.reader = StreamDataReader(reader_format=metadata.reader.type, input=metadata.reader.input, **metadata.reader.config)
        self.transformers = create_transformers(metadata.transformations)
        self.writer = StreamDataWriter(metadata.writer.type, metadata.writer.config, metadata.writer.output)

    def run(self):
        df = self.reader.read(self.spark)

        # Apply each transformer in sequence
        df_transformed = df
        for t in self.transformers:
            df_transformed = t.transform(df_transformed, spark=self.spark)

        self.writer.write(df_transformed)
