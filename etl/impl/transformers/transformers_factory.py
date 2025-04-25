from typing import List

from etl.utils import reflection_utils as ru

from etl.interfaces import DataTransformer
from etl.metadata.schema import TransformerConfig
import etl.impl.transformers.transformers as tr


def create_transformers(transformers: List[TransformerConfig]) -> List[DataTransformer]:
    if transformers is None:
        return [tr.NoOpTransformer()]

    return [load_transformer_from_config(t) for t in transformers]


def load_transformer_from_config(transformer: TransformerConfig) -> DataTransformer:
    cls = ru.load_class_from_string(transformer.klass)
    return cls(**{k: v for k, v in transformer.props.items()}) if transformer.props else cls()

