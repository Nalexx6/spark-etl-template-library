import time
import csv

import etl.utils.spark_utils as su
import json
from bson import json_util
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from datetime import datetime
import logging
import argparse
import os
import random

logging.basicConfig(format='%(asctime)s: %(levelname)s: %(module)s: %(message)s', level=logging.INFO)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='new trips streaming processor',
    )
    # parser.add_argument("--config", type=str, required=True)

    # args = parser.parse_args()
    # config_path = os.path.join(os.path.dirname(__file__), args.config)
    #
    # spark = su.create_spark_session(app_name='simulation', local=True)
    # config = su.load_config(config_path)

    # files = ['fhvhv_tripdata_2023-01.parquet']
    # admin_client = KafkaAdminClient(
    #     bootstrap_servers=config['kafka_brokers'],
    #     client_id='simulation'
    # )

    # topic_list = []
    # topic_list.append(NewTopic(name="nyc-taxi-topic", num_partitions=1, replication_factor=1))
    # admin_client.create_topics(new_topics=topic_list, validate_only=False)

    # df = csv.DictReader("/Users/moleksiienko/University/dlt-deployer/etl/licenses.csv")

    # Initialize KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],  # Replace with your Kafka server
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializing JSON
    )

    # Read CSV file
    csv_file = "/Users/moleksiienko/University/dlt-deployer/etl/licenses.csv"

    with open(csv_file, mode='r') as file:
        reader = csv.DictReader(file)  # Using DictReader to handle rows as dictionaries

        for row in reader:
            # Send each row to Kafka as a JSON message
            producer.send('data_topic', value=row)

    # Ensure all messages are sent before closing
    producer.flush()
    producer.close()
