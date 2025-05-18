from pyspark.sql import SparkSession

from etl.metadata.pipeline_schema import PipelineMetadata
from etl.registry import Registry

from etl.runner import batch_runner as bd
from etl.runner import streaming_runner as st


def create_runner(runner_type: str, spark: SparkSession, metadata: PipelineMetadata, registry: Registry):
    if runner_type == "batch":
        return bd.BatchPipelineRunner(spark, metadata, registry)
    else:
        return st.StreamPipelineRunner(spark, metadata, registry)
