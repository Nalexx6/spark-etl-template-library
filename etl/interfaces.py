from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, df: DataFrame, spark: SparkSession = None) -> DataFrame:
        """Apply transformations to the input DataFrame"""
        pass


class DataWriter(ABC):

    def __call__(self, df: DataFrame, epoch_id) -> None:
        self.write(df)

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass


class DataInput(ABC):
    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        """Read raw data from configured input source and return a Spark DataFrame"""
        pass


class DataOutput(ABC):
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write the processed DataFrame to a destination"""
        pass
