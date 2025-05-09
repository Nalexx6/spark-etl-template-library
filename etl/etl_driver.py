
import argparse, logging

from etl.utils import spark_utils as su
from etl.runner import batch_runner as bd
from etl.runner import streaming_runner as st


from etl.metadata.parser import load_pipeline_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run(config_path: str):

    metadata = load_pipeline_metadata(config_path)

    spark = su.create_spark_session(metadata.name, local=True)

    if metadata.type == "batch":
        driver = bd.BatchPipelineRunner(spark, metadata)
    else:
        driver = st.StreamPipelineRunner(spark, metadata)

    try:
        driver.run()
        logger.info(f"{metadata.name} task completed successfully.")
    except ValueError as e:
        logger.error(f"Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise e


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run ETL pipeline from config")
    parser.add_argument("--config", required=True, help="Path to YAML pipeline config file.")
    args = parser.parse_args()


    run(args.config)