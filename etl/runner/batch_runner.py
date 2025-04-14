from etl.interfaces import DataInput, DataTransformer, DataOutput
from pyspark.sql import SparkSession


class BatchPipelineRunner:
    def __init__(self, spark: SparkSession, reader: DataInput, transformer: DataTransformer, writer: DataOutput):
        self.spark = spark
        self.reader = reader
        self.transformer = transformer
        self.writer = writer

    def run(self):
        df = self.reader.read(self.spark)
        df_processed = self.transformer.transform(df)
        self.writer.write(df_processed)

