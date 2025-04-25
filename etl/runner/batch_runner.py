from typing import List

from etl.interfaces import DataReader, DataTransformer, DataWriter
from pyspark.sql import SparkSession


class BatchPipelineRunner:
    def __init__(self, spark: SparkSession, reader: DataReader, transformers: List[DataTransformer], writer: DataWriter):
        self.spark = spark
        self.reader = reader
        self.transformers = transformers
        self.writer = writer

    def run(self):
        df = self.reader.read(self.spark)

        # Apply each transformer in sequence
        df_transformed = df
        for t in self.transformers:
            df_transformed = t.transform(df_transformed)

        self.writer.write(df_transformed)

