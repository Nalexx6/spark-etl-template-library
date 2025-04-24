from etl.interfaces import DataWriter
import etl.impl.writers.writers as wr

WRITER_REGISTRY = {
    "s3": wr.S3Writer,
    # TODO: refactor to add
}


def create_writer(writer_type: str, output_format: str, config: dict) -> DataWriter:
    connector_cls = WRITER_REGISTRY.get(writer_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported input connector type: {writer_type}")
    return connector_cls(output_format=output_format, **config)
