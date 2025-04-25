from etl.interfaces import DataReader
from etl.impl.readers.batch.s3_data_reader import S3Reader

INPUT_CONNECTOR_REGISTRY = {
    "s3": S3Reader,
    # TODO: refactor to add
    # "s3": S3Input,
    # "kafka": KafkaInput,
    # "hdfs": HDFSInput,
}


def create_reader(reader_type: str, reader_config: dict,  input_format: str, input_config: dict) -> DataReader:
    connector_cls = INPUT_CONNECTOR_REGISTRY.get(reader_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {reader_type}")
    return connector_cls(input_format=input_format, input_config=input_config, **reader_config)
