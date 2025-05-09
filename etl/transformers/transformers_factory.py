from typing import List
import logging

from etl.utils import reflection_utils as ru

from etl.interfaces import DataTransformer
from etl.metadata.pipeline_schema import TransformerConfig
import etl.transformers.transformers as tr


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_transformers(transformers: List[TransformerConfig]) -> List[DataTransformer]:
    if transformers is None:
        return [tr.NoOpTransformer()]

    return [load_transformer_from_config(t) for t in transformers]


def load_transformer_from_config(transformer: TransformerConfig) -> DataTransformer:
    logger.info(f"Loading transformer object for canonical name {transformer.klass}")
    cls = ru.load_class_from_string(transformer.klass)
    return cls(**{k: v for k, v in transformer.props.items()}) if transformer.props else cls()

