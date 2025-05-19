import logging

from etl.inputs.input_factory import InputFactory
from etl.readers.batch.cassandra_reader import CassandraReader
from etl.readers.batch.iceberg_reader import IcebergReader
from etl.readers.batch.local_reader import LocalReader
from etl.readers.batch.postgres_reader import PostgresReader
from etl.readers.batch.redshift_reader import RedshiftReader
from etl.interfaces import DataReader
from etl.metadata.pipeline_schema import InputConfig

from etl.readers.batch.s3_data_reader import S3Reader
from etl.readers.batch.hdfs_data_reader import HdfsReader
from etl.readers.batch.kafka_data_reader import KafkaReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReaderFactory:

    def __init__(self, additional_registry: dict[str, DataReader] = None):

        if additional_registry is None:
            additional_registry = {}

        default_registry = {
            "local": LocalReader,
            "s3": S3Reader,
            "hdfs": HdfsReader,
            "kafka": KafkaReader,
            "postgres": PostgresReader,
            "iceberg": IcebergReader,
            "cassandra": CassandraReader,
            "redshift": RedshiftReader,

        }

        self.reader_registry = default_registry | additional_registry

    def create_reader(self, reader_type: str, reader_config: dict,
                      input: InputConfig, input_factory: InputFactory) -> DataReader:

        (input_format, input_config) = (input.format, input.config) if input else (None, {})

        connector_cls = self.reader_registry.get(reader_type.lower())
        if not connector_cls:
            raise ValueError(f"Unsupported input connector type: {reader_type}")

        logger.info(f"Initializing {reader_type} reader object with options: {reader_config},"
                    f" input format: {input_format},"
                    f" input config: {input_config} ")
        return connector_cls(input_format=input_format, input_config=input_config,
                             input_factory=input_factory, **reader_config)
