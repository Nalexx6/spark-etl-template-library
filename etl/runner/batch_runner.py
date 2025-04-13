from etl.interfaces import DataInput, DataProcessor, DataOutput
from pyspark.sql import SparkSession


class BatchPipelineDriver:
    def __init__(self, spark: SparkSession, reader: DataInput, processor: DataProcessor, writer: DataOutput):
        self.spark = spark
        self.reader = reader
        self.processor = processor
        self.writer = writer

    def run(self):
        df = self.reader.read(self.spark)
        df_processed = self.processor.process(df)
        self.writer.write(df_processed)
