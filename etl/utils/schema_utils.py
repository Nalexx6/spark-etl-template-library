import json
from pyspark.sql import functions as f
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame


def decode_json_value(df, schema: StructType) -> DataFrame:
    """
    Decodes Kafka 'value' (binary) field assuming it's JSON format.
    """
    return df.withColumn("temp_value", f.from_json(f.col("value").cast("string"), schema)) \
                .drop("value") \
                .withColumnRenamed("temp_value", "value")


def decode_avro_value(df, avro_schema: str) -> DataFrame:
    """
    Decodes Kafka 'value' field assuming it's Avro-encoded.
    """
    return df.withColumn("temp_value", f.from_avro("value", avro_schema)) \
             .drop("value") \
             .withColumnRenamed("temp_value", "value")


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

