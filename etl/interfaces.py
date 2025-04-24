from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession


class DataReader(ABC):

    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        """
        Read raw data using the configured reader, apply post-read transformations to it, if needed.
        Return a Spark DataFrame
        """

        pass


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply transformations to the input DataFrame"""
        pass


class DataWriter(ABC):

    @abstractmethod
    def write(self, df: DataFrame):
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
