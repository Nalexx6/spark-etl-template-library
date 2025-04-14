from etl.interfaces import DataInput
import etl.impl.readers.readers as rd

INPUT_CONNECTOR_REGISTRY = {
    "csv": rd.CsvInput,
    "parquet": rd.ParquetInput,
    # TODO: refactor to add
    # "s3": S3Input,
    # "kafka": KafkaInput,
    # "hdfs": HDFSInput,
}

def create_input_connector(input_type: str, config: dict) -> DataInput:
    connector_cls = INPUT_CONNECTOR_REGISTRY.get(input_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {input_type}")
    return connector_cls(**config)
