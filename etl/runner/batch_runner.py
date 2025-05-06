from typing import List

from etl.impl.readers.reader_interface import DataReader
from etl.interfaces import DataTransformer, DataWriter
from pyspark.sql import SparkSession

from etl.metadata.pipeline_schema import PipelineMetadata

from etl.impl.transformers.transformers_factory import create_transformers
from etl.impl.readers.reader_factory import create_reader
from etl.impl.writers.writer_factory import create_writer


class BatchPipelineRunner:
    def __init__(self, spark: SparkSession, metadata: PipelineMetadata):

        self.spark = spark

        self.reader = create_reader(metadata.reader.type, metadata.reader.config,
                                    metadata.reader.input)
        self.transformers = create_transformers(metadata.transformations)
        self.writer = create_writer(metadata.writer.type, metadata.writer.config,
                                    metadata.writer.output)

    def run(self):
        df = self.reader.read(self.spark)

        # Apply each transformer in sequence
        df_transformed = df
        for t in self.transformers:
            df_transformed = t.transform(df_transformed, spark=self.spark)

        self.writer.write(df_transformed)

