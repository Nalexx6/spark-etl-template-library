from pydantic import BaseModel
from typing import List, Optional, Literal, Dict


class InputConfig(BaseModel):
    format: Literal["csv", "parquet"]
    config: Dict


class ReaderConfig(BaseModel):
    type: Literal["s3", "hdfs", "kafka"]
    config: Dict
    input: InputConfig


class OutputConfig(BaseModel):
    format: Literal["csv", "parquet"]
    config: Dict


class WriterConfig(BaseModel):
    type: Literal["s3", "hdfs", "kafka", "redshift", "cassandra"]
    config: Dict
    output: OutputConfig


class TransformerConfig(BaseModel):
    klass: Optional[str] = None
    props: Optional[Dict[str, str]] = None


class PipelineMetadata(BaseModel):
    name: str
    reader: ReaderConfig
    # TODO: support chain of transformers
    transformations: Optional[List[TransformerConfig]]
    writer: WriterConfig
