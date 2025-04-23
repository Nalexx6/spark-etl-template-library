from etl.interfaces import DataReader
import etl.impl.readers.readers as rd

INPUT_CONNECTOR_REGISTRY = {
    "s3": rd.S3Reader,
    # TODO: refactor to add
    # "s3": S3Input,
    # "kafka": KafkaInput,
    # "hdfs": HDFSInput,
}


def create_reader(reader_type: str, input_format: str, config: dict) -> DataReader:
    connector_cls = INPUT_CONNECTOR_REGISTRY.get(reader_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {reader_type}")
    return connector_cls(input_format=input_format, **config)
