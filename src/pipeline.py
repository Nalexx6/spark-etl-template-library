from abc import ABC, abstractmethod
from typing import Dict, Any

class DLTSource(ABC):
    """
    Abstract base class for data sources.
    """
    @abstractmethod
    def extract(self, config: Dict[str, Any]):
        """
        Extracts data from the source based on the provided configuration.
        Returns the extracted data in a format suitable for processing (e.g., a Spark DataFrame).
        """
        pass

class DLTTarget(ABC):
    """
    Abstract base class for data targets.
    """
    @abstractmethod
    def load(self, data: Any, config: Dict[str, Any]):
        """
        Loads the processed data into the target based on the provided configuration.
        Args:
            data: The data to load (e.g., a Spark DataFrame).
        """
        pass

class DLTTransformation(ABC):
    """
    Abstract base class for data transformations.
    """
    @abstractmethod
    def transform(self, data: Any, config: Dict[str, Any]):
        """
        Transforms the input data based on the provided configuration.
        Args:
            data: The data to transform (e.g., a Spark DataFrame).
            config: Configuration for the transformation.
        Returns:
            The transformed data.
        """
        pass

class DLTTask(ABC):
    """
    Abstract base class for a single DLT task (combining extract, transform, load).
    """
    @abstractmethod
    def run(self, config: Dict[str, Any]):
        """
        Executes the DLT task using the provided configuration.
        Args:
            config: A dictionary containing configuration parameters for the task
                    (e.g., source connection details, transformation logic,
                    target connection details).
        """
        pass

class DLTPipeline(ABC):
    """
    Abstract base class for a DLT pipeline, which can consist of multiple tasks.
    """
    @abstractmethod
    def run(self, config: Dict[str, Any]):
        """
        Executes the entire DLT pipeline using the provided configuration.
        Args:
            config: A dictionary containing configuration parameters for the pipeline
                    and its individual tasks.
        """
        pass