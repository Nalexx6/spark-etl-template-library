from pydantic import BaseModel
from typing import List, Optional, Literal, Dict, Union


class InputConfig(BaseModel):
    type: Literal["s3", "hdfs", "kafka"]
    format: Literal["csv", "parquet"]
    config: Dict


class OutputConfig(BaseModel):
    type: Literal["s3", "hdfs", "kafka", "redshift", "cassandra", "csv", "parquet"]
    format: Optional[str]
    config: Dict


class TransformerConfig(BaseModel):
    klass: Optional[str] = None
    props: Optional[Dict[str, str]] = None


class PipelineMetadata(BaseModel):
    name: str
    input: InputConfig
    # TODO: support chain of transformers
    transformations: Optional[TransformerConfig]
    output: OutputConfig
