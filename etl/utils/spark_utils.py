import os

from pyspark.sql import SparkSession


def create_spark_session(app_name, local=False):
    packages = [
        # 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1',
        # 'org.apache.kafka:kafka-clients:2.8.0',
        # 'org.postgresql:postgresql:42.5.1',
        'org.apache.hadoop:hadoop-aws:3.3.4'
    ]

    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    defaults = {
        'spark.hadoop.fs.s3a.endpoint': 'http://s3.eu-west-1.amazonaws.com',
        'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        'spark.sql.session.timeZone': 'UTC',
        'spark.default.parallelism': 20,
        "spark.jars.packages": ",".join(packages),
        "spark.hadoop.fs.s3a.access.key": aws_access_key_id,
        "spark.hadoop.fs.s3a.secret.key": aws_secret_access_key

    }

    builder = (SparkSession
               .builder
               .appName(app_name)
               )

    if local:
        builder = builder.master('local')

    for key, value in {**defaults}.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
