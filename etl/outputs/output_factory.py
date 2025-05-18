import logging

from etl.interfaces import DataOutput
import etl.outputs.outputs as op

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OutputFactory:

    def __init__(self, additional_registry: dict[str, DataOutput] = None):
        if additional_registry is None:
            additional_registry = {}

        default_registry = {
            "csv": op.CsvOutput,
            "parquet": op.ParquetOutput,
            "delta": op.DeltaLakeOutput
        }

        self.output_registry = default_registry | additional_registry

    def create_output_connector(self, output_format: str, **kwargs) -> DataOutput:
        connector_cls = self.output_registry.get(output_format.lower())
        if not connector_cls:
            raise ValueError(f"Unsupported input connector type: {output_format}")

        logger.info(f"Initializing {output_format} output connector with options {kwargs}")
        return connector_cls(**kwargs)
