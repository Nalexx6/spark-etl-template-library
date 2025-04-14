from etl.interfaces import DataOutput
import etl.impl.writers.writers as wr

OUTPUT_CONNECTOR_REGISTRY = {
    "csv": wr.CsvOutput,
    "parquet": wr.ParquetOutput,
    # TODO:
    # "s3": S3Output,
    # "kafka": KafkaOutput,
    # "hdfs": HDFSOutput,
}

def create_output_connector(output_type: str, config: dict) -> DataOutput:
    connector_cls = OUTPUT_CONNECTOR_REGISTRY.get(output_type.lower())
    if not connector_cls:
        raise ValueError(f"Unsupported output connector type: {output_type}")
    return connector_cls(**config)
