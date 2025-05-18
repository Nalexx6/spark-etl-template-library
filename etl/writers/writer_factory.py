import logging

from etl.interfaces import DataWriter
from etl.metadata.pipeline_schema import OutputConfig
from etl.outputs.output_factory import OutputFactory
from etl.writers.batch.cassandra_writer import CassandraWriter
from etl.writers.batch.console_writer import ConsoleWriter
from etl.writers.batch.hdfs_writer import HdfsWriter
from etl.writers.batch.iceberg_writer import IcebergWriter
from etl.writers.batch.kafka_writer import KafkaWriter
from etl.writers.batch.local_writer import LocalWriter
from etl.writers.batch.postgres_writer import PostgresWriter
from etl.writers.batch.redshift_writer import RedshiftWriter
from etl.writers.batch.s3_writer import S3Writer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WriterFactory:

    def __init__(self, additional_registry: dict[str, DataWriter] = None):

        if additional_registry is None:
            additional_registry = {}

        default_registry = {
            "local": LocalWriter,
            "console": ConsoleWriter,
            "s3": S3Writer,
            "hdfs": HdfsWriter,
            "kafka": KafkaWriter,
            "postgres": PostgresWriter,
            "iceberg": IcebergWriter,
            "cassandra": CassandraWriter,
            "redshift": RedshiftWriter,
        }

        self.writer_registry = default_registry | additional_registry

    def create_writer(self, writer_type: str, writer_config: dict,
                      output: OutputConfig, output_factory: OutputFactory) -> DataWriter:

        (output_format, output_config) = (output.format, output.config) if output else (None, None)

        connector_cls = self.writer_registry.get(writer_type.lower())
        if not connector_cls:
            raise ValueError(f"Unsupported input connector type: {writer_type}")

        logger.info(f"Initializing {writer_type} writer object with options: {writer_config},"
                    f" output format: {output_format},"
                    f" output config: {output_config} ")
        return connector_cls(output_format=output_format, output_config=output_config,
                             output_factory=output_factory, **writer_config)
