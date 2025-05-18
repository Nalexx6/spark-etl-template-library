
import logging

import etl.utils as ut
from etl.registry import Registry
from etl.utils import spark_utils as su
from etl.runner import create_runner


from etl.metadata import load_pipeline_metadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLDriver:

    def __init__(self):
        args = ut.get_etl_args()

        self.registry = Registry(args.registry)

        self.metadata = load_pipeline_metadata(args.config)

        self.spark = su.create_spark_session(self.metadata.name, local=True, add_spark_conf_path=args.spark_conf)

        self.runner = create_runner(self.metadata.type, self.spark, self.metadata, self.registry)

    def run(self) -> None:
        try:
            logger.info(f"{self.metadata.name} task started.")
            self.runner.run()
            logger.info(f"{self.metadata.name} task completed successfully.")
        except ValueError as e:
            logger.error(f"Error: {e}")
            raise e
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise e


if __name__ == "__main__":
    ETLDriver().run()

