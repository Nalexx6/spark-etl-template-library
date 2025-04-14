from etl.utils import spark_utils as su, reflection_utils as ru
from etl.runner import batch_runner as bd

from etl.metadata.parser import load_pipeline_metadata
from etl.impl.readers.reader_factory import create_input_connector
from etl.impl.writers.writer_factory import create_output_connector
from etl.impl.transformers.transformers_factory import create_transformer

from etl.impl.transformers.transformers import NoOpTransformer

if __name__ == "__main__":

    metadata = load_pipeline_metadata("examples/dummy_test.yaml")

    input_obj = create_input_connector(metadata.input.type, metadata.input.config)
    output_obj = create_output_connector(metadata.output.type, metadata.output.config)
    transformer_obj = create_transformer(metadata.transformations)

    spark = su.create_spark_session(metadata.name, local=True)

    driver = bd.BatchPipelineRunner(spark=spark, reader=input_obj, transformer=transformer_obj, writer=output_obj)

    driver.run()