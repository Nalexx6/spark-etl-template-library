import argparse, logging

from etl.utils import spark_utils as su
from etl.runner import batch_runner as bd

from etl.metadata.parser import load_pipeline_metadata
from etl.impl.readers.reader_factory import create_reader
from etl.impl.writers.writer_factory import create_output_connector
from etl.impl.transformers.transformers_factory import create_transformer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run ETL pipeline from config")
    parser.add_argument("--config", required=True, help="Path to YAML pipeline config file.")
    args = parser.parse_args()

    metadata = load_pipeline_metadata(args.config)

    input_obj = create_reader(metadata.input.type, metadata.input.format, metadata.input.config)
    output_obj = create_output_connector(metadata.output.type, metadata.output.config)
    transformer_obj = create_transformer(metadata.transformations)

    spark = su.create_spark_session(metadata.name, local=True)

    driver = bd.BatchPipelineRunner(spark=spark, reader=input_obj, transformer=transformer_obj, writer=output_obj)

    try:
        driver.run()
        logger.info(f"{metadata.name} task completed successfully.")
    except ValueError as e:
        logger.error(f"Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise e
