from etl.utils import spark_utils as su
from etl.runner import batch_runner as bd
from etl.impl.dummy import DummyCsvInput, NoOpProcessor, DummyCsvOutput
from typing import Dict, Any
from datetime import datetime

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(config: Dict[str, Any]):
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

        reader = DummyCsvInput(source_path)
        writer = DummyCsvOutput(source_path)

        driver = bd.BatchPipelineDriver(spark=spark, reader=reader, processor=NoOpProcessor(), writer=writer)

        driver.run()

    finally:
        spark.stop()


if __name__ == "__main__":
    cur_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Example usage (for local testing)
    dummy_config = {
        "source_path": "s3a://nalexx6-dlt-bucket/licenses.csv",  # Replace with your actual source S3 path
        "destination_path": f"s3a://nalexx6-dlt-bucket/licenses-processed-{cur_timestamp}.csv"  # Replace with your actual destination S3 path
    }

    try:
        run(dummy_config)
        logger.info("Dummy S3 to S3 task completed successfully.")
    except ValueError as e:
        logger.error(f"Error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")