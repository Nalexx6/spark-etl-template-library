import time

from src.pipeline import DLTTask
from src.utils import spark_utils as su
from typing import Dict, Any
from datetime import datetime

class DummyS3ToS3Task(DLTTask):
    """
    A dummy DLT task that reads data from one S3 location and writes to another.
    """
    def run(self, config: Dict[str, Any]):
        """
        Reads from source S3, performs no transformation, and writes to destination S3.
        Args:
            config: Dictionary with 'source_path' and 'destination_path' keys.
        """
        source_path = config.get("source_path")
        destination_path = config.get("destination_path")

        if not source_path or not destination_path:
            raise ValueError("Both 'source_path' and 'destination_path' must be provided in the configuration.")

        spark = su.create_spark_session("DummyS3ToS3", local=True)

        try:
            # Read from source S3
            df = spark.read.csv(source_path, header=True, inferSchema=True)
            print(f"Successfully read from: {source_path}")
            df.show()

            # Write to destination S3
            df.write.mode("overwrite").csv(destination_path, header=True)
            print(f"Successfully written to: {destination_path}")

        finally:
            spark.stop()

if __name__ == "__main__":
    cur_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Example usage (for local testing)
    dummy_config = {
        "source_path": "s3a://nalexx6-dlt-bucket/licenses.csv",  # Replace with your actual source S3 path
        "destination_path": f"s3a://nalexx6-dlt-bucket/licenses-processed-{cur_timestamp}.csv"  # Replace with your actual destination S3 path
    }

    task = DummyS3ToS3Task()
    try:
        task.run(dummy_config)
        print("Dummy S3 to S3 task completed successfully.")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")