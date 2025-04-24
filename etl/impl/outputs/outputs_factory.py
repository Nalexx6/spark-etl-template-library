from etl.interfaces import DataOutput
import etl.impl.outputs.outputs as op

INPUT_CONNECTOR_REGISTRY = {
    "csv": op.CsvOutput,
    "parquet": op.ParquetOutput,
    # TODO: add more
}


def create_output_connector(output_format: str, **kwargs) -> DataOutput:
    connector_cls = INPUT_CONNECTOR_REGISTRY.get(output_format.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {output_format}")
    return connector_cls(**kwargs)
