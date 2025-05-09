from etl.metadata.pipeline_schema import PipelineMetadata
from etl.utils import load_yaml


def load_pipeline_metadata(path: str) -> PipelineMetadata:
    return PipelineMetadata(**load_yaml(path)["pipeline"])