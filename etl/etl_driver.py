
import argparse, logging

from etl.utils import spark_utils as su
from etl.runner import batch_runner as bd
from etl.runner import streaming_runner as st


from etl.metadata.parser import load_pipeline_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLDriver:

    def __init__(self):
        parser = argparse.ArgumentParser(description="Run ETL pipeline from config")
        parser.add_argument("--config", required=True, help="Path to YAML pipeline config file.")
        args = parser.parse_args()

        self.metadata = load_pipeline_metadata(args.config)

        self.spark = su.create_spark_session(self.metadata.name, local=True)

        if self.metadata.type == "batch":
            self.driver = bd.BatchPipelineRunner(self.spark, self.metadata)
        else:
            self.driver = st.StreamPipelineRunner(self.spark, self.metadata)

    def run(self) -> None:
        try:
            self.driver.run()
            logger.info(f"{self.metadata.name} task completed successfully.")
        except ValueError as e:
            logger.error(f"Error: {e}")
            raise e
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise e


if __name__ == "__main__":
    ETLDriver().run()

