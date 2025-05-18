from pyspark.sql import SparkSession

from etl.metadata.pipeline_schema import PipelineMetadata
from etl.registry import Registry

from etl.transformers.transformers_factory import create_transformers


class BatchPipelineRunner:
    def __init__(self, spark: SparkSession, metadata: PipelineMetadata, registry: Registry):

        self.spark = spark

        self.reader = registry.reader_factory.create_reader(metadata.reader.type, metadata.reader.config,
                                    metadata.reader.input, registry.input_factory)
        self.transformers = create_transformers(metadata.transformations)
        self.writer = registry.writer_factory.create_writer(metadata.writer.type, metadata.writer.config,
                                    metadata.writer.output, registry.output_factory)

    def run(self):
        df = self.reader.read(self.spark)

        # Apply each transformer in sequence
        df_transformed = df
        for t in self.transformers:
            df_transformed = t.transform(df_transformed, spark=self.spark)

        self.writer.write(df_transformed)

