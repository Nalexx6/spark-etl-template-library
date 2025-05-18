import logging

from etl.interfaces import DataInput
import etl.inputs.inputs as ip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InputFactory:

    def __init__(self, additional_registry: dict[str, DataInput] = None):
        if additional_registry is None:
            additional_registry = {}

        default_registry = {
            "csv": ip.CsvInput,
            "parquet": ip.ParquetInput,
            "delta": ip.DeltaLakeInput
        }

        self.input_registry = default_registry | additional_registry

    def create_input_connector(self, input_format: str, **kwargs) -> DataInput:
        connector_cls = self.input_registry.get(input_format.lower())
        if not connector_cls:
            raise ValueError(f"Unsupported input connector type: {input_format}")

        logger.info(f"Initializing {input_format} input connector with options: {kwargs}")
        return connector_cls(**kwargs)
