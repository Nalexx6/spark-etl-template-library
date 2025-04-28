from pydantic import BaseModel
from typing import List, Optional, Literal, Dict


class InputConfig(BaseModel):
    format: Literal["csv", "parquet", "avro", "json"]
    config: Optional[Dict] = {}


class ReaderConfig(BaseModel):
    type: Literal["s3", "hdfs", "kafka"]
    config: Optional[Dict] = {}
    input: Optional[InputConfig] = None


class OutputConfig(BaseModel):
    format: Literal["csv", "parquet", "avro", "json"]
    config: Optional[Dict] = {}


class WriterConfig(BaseModel):
    type: Literal["console", "s3", "hdfs", "kafka"]
    config: Optional[Dict] = {}
    output: Optional[OutputConfig] = None


class TransformerConfig(BaseModel):
    klass: str
    props: Optional[Dict[str, str]] = {}


class PipelineMetadata(BaseModel):
    name: str
    type: Literal["batch", "stream"]
    reader: ReaderConfig
    # TODO: support chain of transformers
    transformations: Optional[List[TransformerConfig]] = []
    writer: WriterConfig
