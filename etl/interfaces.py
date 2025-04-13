from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class DataInput(ABC):
    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        """Read data and return a Spark DataFrame"""
        pass


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply transformations to the input DataFrame"""
        pass


class DataOutput(ABC):
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write the processed DataFrame to a destination"""
        pass
