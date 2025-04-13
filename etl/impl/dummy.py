from etl.interfaces import DataInput, DataProcessor, DataOutput
from pyspark.sql import DataFrame, SparkSession

class DummyCsvInput(DataInput):
    def __init__(self, input_path: str):
        self.input_path = input_path

    def read(self, spark: SparkSession) -> DataFrame:
        df = spark.read.csv(self.input_path, header=True, inferSchema=True)
        print(f"Successfully read from: {self.input_path}")
        return df


class NoOpProcessor(DataProcessor):
    def process(self, df: DataFrame) -> DataFrame:
        return df  # No transformation


class DummyCsvOutput(DataOutput):
    def __init__(self, output_path: str):
        self.output_path = output_path

    def write(self, df: DataFrame) -> None:
        df.write.mode("overwrite").csv(self.output_path, header=True)
        print(f"Successfully written to: {self.output_path}")