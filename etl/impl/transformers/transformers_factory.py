from etl.utils import reflection_utils as ru

from etl.interfaces import DataTransformer
from etl.metadata.schema import TransformerConfig
import etl.impl.transformers.transformers as tr


def create_transformer(transformer: TransformerConfig) -> DataTransformer:
    if transformer is None:
        return tr.NoOpTransformer()

    cls = ru.load_class_from_string(transformer.klass)
    return cls(**{k: v for k, v in transformer.props.items()}) if transformer.props else cls()

