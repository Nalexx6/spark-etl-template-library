import os
import logging

from pyspark.sql import SparkSession

from etl.utils import load_yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str, add_spark_conf_path: str = None, local=False):
    default_packages = [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0',
        'org.postgresql:postgresql:42.5.1',
        "org.apache.spark:spark-avro_2.12:3.4.0",
        "org.postgresql:postgresql:42.5.1",
    ]

    if add_spark_conf_path:
        add_spark_conf = load_yaml(add_spark_conf_path)
    else:
        add_spark_conf = {}

    if add_spark_conf.get("use_builtin", True):
        packages = default_packages + add_spark_conf.pop("spark.jars.packages", [])
        spark_conf = {
            'spark.hadoop.fs.s3a.endpoint': 'http://s3.eu-west-1.amazonaws.com',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.sql.session.timeZone': 'UTC',
            'spark.default.parallelism': 20,
            "spark.jars.packages": ",".join(packages),
            "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", "no-access-key-provided"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", "no-secret-access-key-provided"),
            **add_spark_conf
        }
    else:
        packages = add_spark_conf.pop("spark.jars.packages", [])
        spark_conf = {
            "spark.jars.packages": ",".join(packages),
            **add_spark_conf
        }

    builder = (SparkSession
               .builder
               .appName(app_name)
               )

    if local:
        builder = builder.master('local')

    for key, value in {**spark_conf}.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
