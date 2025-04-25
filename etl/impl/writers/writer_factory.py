from etl.interfaces import DataWriter
from etl.metadata.schema import OutputConfig
import etl.impl.writers.writers as wr

WRITER_REGISTRY = {
    "console": wr.ConsoleWriter,
    "s3": wr.S3Writer,
    # TODO: refactor to add
}


def create_writer(writer_type: str, writer_config: dict, output: OutputConfig) -> DataWriter:

    (output_format, output_config) = (output.format, output.config) if output else (None, None)

    connector_cls = WRITER_REGISTRY.get(writer_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {writer_type}")
    return connector_cls(output_format=output_format, output_config=output_config, **writer_config)
