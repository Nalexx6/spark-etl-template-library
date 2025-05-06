import logging

from etl.impl.readers.reader_interface import DataReader
from etl.metadata.pipeline_schema import InputConfig

from etl.impl.readers.batch.s3_data_reader import S3Reader
from etl.impl.readers.batch.hdfs_data_reader import HdfsReader
from etl.impl.readers.batch.kafka_data_reader import KafkaReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

READER_REGISTRY = {
    "s3": S3Reader,
    "hdfs": HdfsReader,
    "kafka": KafkaReader
}


def create_reader(reader_type: str, reader_config: dict,  input: InputConfig) -> DataReader:

    (input_format, input_config) = (input.format, input.config) if input else (None, None)

    connector_cls = READER_REGISTRY.get(reader_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {reader_type}")

    logger.info(f"Initializing {reader_type} reader object with options: {reader_config},"
                f" input format: {input_format},"
                f" input config: {input_config} ")
    return connector_cls(input_format=input_format, input_config=input_config, **reader_config)
