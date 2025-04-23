from etl.interfaces import DataInput
import etl.impl.inputs.inputs as ip

INPUT_CONNECTOR_REGISTRY = {
    "csv": ip.CsvInput,
    "parquet": ip.ParquetInput,
    # TODO: add more
}


def create_input_connector(input_format: str, **kwargs) -> DataInput:
    connector_cls = INPUT_CONNECTOR_REGISTRY.get(input_format.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {input_format}")
    return connector_cls(**kwargs)
