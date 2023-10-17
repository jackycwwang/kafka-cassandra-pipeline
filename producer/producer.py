import logging
import json
import time
import os
# import threading
# from decimal import *
import time
# from uuid import uuid4, UUID
from datetime import datetime
import yaml
# import pytz

# import pandas as pd

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
# from confluent_kafka.serialization import StringSerializer

import hydra
from omegaconf import OmegaConf, DictConfig


log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger("producer")

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        logger.error(
            "Delivery failed for record {}: {}".format(msg.key(), err))
        return
    logger.info('Update record [{}] successfully produced to topic {} partition [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


@hydra.main(version_base=None, config_path='conf', config_name='config')
def producer_app(cfg: DictConfig) -> None:
    sr_cfg = cfg['schema_registry']
    kafka_cfg = cfg['kafka']


    # ----------- Make the connections to Kafka cluster and Schema Registry -----------
    # Create a Schema Registry client
    schema_registry_config = {
        'url': sr_cfg['schema.registry.url'],
        'basic.auth.user.info': '{}:{}'.format(sr_cfg['basic.auth.credentials.source'], sr_cfg['basic.auth.user.info'])
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_config)

    # Fetch the latest Avro schema for the key and the value
    key_subject_name = sr_cfg["subject.key"]
    value_subject_name = sr_cfg["subject.value"]
    try:
        key_schema_str = schema_registry_client.get_latest_version(
            key_subject_name).schema.schema_str
        value_schema_str = schema_registry_client.get_latest_version(
            value_subject_name).schema.schema_str
        logger.info(f"Schemas have been fetched from the registry")
        print(f'key_schema_str:{key_schema_str}, value_schema_str: {value_schema_str}')
    except Exception as e:
        logger.error(f" Fetching schema error: {e}")

    # Create Avro Serializer for the key and the value
    key_avro_serializer = AvroSerializer(schema_registry_client, key_schema_str)
    value_avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)

    # Define Kafka configuration and create a kafka producer
    kafka_config = {
        'bootstrap.servers': kafka_cfg['bootstrap.servers'],
        'sasl.mechanisms': kafka_cfg['sasl.mechanisms'],
        'security.protocol': kafka_cfg['security.protocol'],
        'sasl.username': kafka_cfg['sasl.username'],
        'sasl.password': kafka_cfg['sasl.password'],
        'key.serializer': key_avro_serializer,
        'value.serializer': value_avro_serializer
    }
    try:
        producer = SerializingProducer(kafka_config)
        logger.info("Kafka connection has been successfully established!")
    except Exception as err:
        logger.error(f"Kafka cluster connection error: {err}")

    try:
        time.sleep(5.0)
        while True:
            # Fetch all rows from the result set
            rows = []

            # Iterate through the rows and print the values
            if rows:
                for row in rows:
                    id, name, category, price, last_updated = row
                    # logger.info(f"Last_read object: {last_read_obj}")
                    # logger.info(f"Last updated object: {last_updated}")
                    logger.info(
                        f"ID: {id}, Name: {name}, Category: {category}, Price: {price}, Last Updated: {last_updated}")
                    # last_updated = last_updated.astimezone(tz)

                    key = {

                    }
                    value = {
                        'id': id,
                        'name': name,
                        'category': category,
                        'price': price,
                        'last_updated': last_updated.strftime('%Y-%m-%d %H:%M:%S')
                    }

                    producer.produce(topic=kafka_cfg["topic"],
                                     key=key,
                                     value=value,
                                     on_delivery=delivery_report)
                    producer.flush()  # may become blocking in a long run
