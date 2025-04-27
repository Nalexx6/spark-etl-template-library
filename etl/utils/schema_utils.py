import json
from typing import Callable

from pyspark.sql import functions as f
from pyspark.sql.avro.functions import to_avro, from_avro
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame


def decode_value(format: str, schema_filepath: str) -> Callable[[DataFrame], DataFrame]:
    def wrapper(df: DataFrame) -> DataFrame:
        if format.lower() == "json":
            schema = load_json_schema(schema_filepath)
            df_decoded = df.withColumn("temp_value", f.from_json(f.col("value").cast("string"), schema))
        elif format.lower() == "avro":
            schema = load_avro_schema(schema_filepath)
            df_decoded = df.withColumn("temp_value", from_avro("value", schema))
        else:
            raise ValueError(f"Unsupported format '{format}'. Only 'json' and 'avro' are supported.")

        return df_decoded.drop("value").withColumnRenamed("temp_value", "value")

    return wrapper


def encode_value(format: str, schema_filepath: str) -> Callable[[DataFrame], DataFrame]:
    def wrapper(df: DataFrame) -> DataFrame:
        if format.lower() == "json":
            schema = load_json_schema(schema_filepath)
            field_names = [field.name for field in schema.fields]
            df_encoded = df.withColumn("value", f.to_json(f.struct(*field_names)))
        elif format.lower() == "avro":
            df_encoded = df.withColumn("value", to_avro(f.struct([col for col in df.columns]),
                                                         load_avro_schema(schema_filepath)))
        else:
            raise ValueError(f"Unsupported format '{format}'. Only 'json' and 'avro' are supported.")

        return df_encoded.select("value")

    return wrapper


def load_json_schema(file_path: str) -> StructType:
    """
    Loads a JSON schema from a file and converts it into PySpark StructType.
    """
    with open(file_path, "r") as file:
        schema_json = json.load(file)
    return StructType.fromJson(schema_json)


def load_avro_schema(file_path: str) -> str:
    """
    Loads an Avro schema from a file and returns it as a raw JSON string.
    """
    with open(file_path, "r") as file:
        schema_str = file.read()
    return schema_str
