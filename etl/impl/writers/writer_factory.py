import logging

from etl.interfaces import DataWriter
from etl.metadata.pipeline_schema import OutputConfig
import etl.impl.writers.writers as wr


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WRITER_REGISTRY = {
    "console": wr.ConsoleWriter,
    "s3": wr.S3Writer,
    "hdfs": wr.HdfsWriter,
    "kafka": wr.KafkaWriter
}


def create_writer(writer_type: str, writer_config: dict, output: OutputConfig) -> DataWriter:

    (output_format, output_config) = (output.format, output.config) if output else (None, None)

    connector_cls = WRITER_REGISTRY.get(writer_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {writer_type}")

    logger.info(f"Initializing {writer_type} writer object with options: {writer_config},"
                f" output format: {output_format},"
                f" output config: {output_config} ")
    return connector_cls(output_format=output_format, output_config=output_config, **writer_config)
