from pydantic import BaseModel
from typing import List, Optional, Literal, Dict


class InputConfig(BaseModel):
    format: str
    config: Optional[Dict] = {}


class ReaderConfig(BaseModel):
    type: str
    config: Optional[Dict] = {}
    input: Optional[InputConfig] = None


class OutputConfig(BaseModel):
    format: str
    config: Optional[Dict] = {}


class WriterConfig(BaseModel):
    type: str
    config: Optional[Dict] = {}
    output: Optional[OutputConfig] = None


class TransformerConfig(BaseModel):
    klass: str
    props: Optional[Dict[str, str]] = {}


class PipelineMetadata(BaseModel):
    name: str
    type: Literal["batch", "stream"]
    reader: ReaderConfig
    transformations: Optional[List[TransformerConfig]] = []
    writer: WriterConfig

