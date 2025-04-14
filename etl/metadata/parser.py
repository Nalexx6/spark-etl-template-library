import yaml
from etl.metadata.schema import PipelineMetadata


def load_pipeline_metadata(path: str) -> PipelineMetadata:
    with open(path, "r") as f:
        raw_yaml = yaml.safe_load(f)
    return PipelineMetadata(**raw_yaml["pipeline"])
