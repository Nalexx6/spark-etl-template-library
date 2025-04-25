from etl.interfaces import DataReader
from etl.metadata.schema import InputConfig
from etl.impl.readers.batch.s3_data_reader import S3Reader
from etl.impl.readers.batch.hdfs_data_reader import HdfsReader


INPUT_CONNECTOR_REGISTRY = {
    "s3": S3Reader,
    "hdfs": HdfsReader
}


def create_reader(reader_type: str, reader_config: dict,  input: InputConfig) -> DataReader:

    (input_format, input_config) = (input.format, input.config) if input else (None, None)

    connector_cls = INPUT_CONNECTOR_REGISTRY.get(reader_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {reader_type}")
    return connector_cls(input_format=input_format, input_config=input_config, **reader_config)
