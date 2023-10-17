import os
import logging
import json
import threading

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
# from confluent_kafka.serialization import StringDeserializer

import hydra
from omegaconf import OmegaConf, DictConfig


consumer_name = os.getenv('CONSUMER_NAME')

log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger("consumer_logger")


@hydra.main(version_base=None, config_path='conf', config_name='config')
def consumer_app(cfg: DictConfig):
    sr_cfg = cfg['schema_registry']
    kafka_cfg = cfg['kafka']

    # ---------- Create a Schema Registry client ----------
    schema_registry_client = SchemaRegistryClient({
      'url': sr_cfg['url'],
      'basic.auth.user.info': '{}:{}'.format(sr_cfg['user'], sr_cfg['secret'])
    })

    # Fetch the latest Avro schema for the value
    # Fetch the latest Avro schema for the key and the value
    subject_name = 'ecommerce-orders-key'
    key_schema_str = schema_registry_client.get_latest_version(
        subject_name).schema.schema_str
    subject_name = 'ecommerce-orders-value'
    value_schema_str = schema_registry_client.get_latest_version(
        subject_name).schema.schema_str
    # print('schema_str:',schema_str)

    # Create Avro Serializer for the key and the value
    # key_serializer = StringSerializer('utf_8')
    key_avro_serializer = AvroSerializer(schema_registry_client, key_schema_str)
    value_avro_serializer = AvroSerializer(schema_registry_client, value_schema_str)

    # ---------- Create a Kafka consumer -------------
    # key_deserializer = StringDeserializer('utf_8')
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    # Define Kafka configuration
    kafka_config = {
        'bootstrap.servers': kafka_cfg['bootstrap_servers'],
        'sasl.mechanisms': kafka_cfg['sasl_mechanisms'],
        'security.protocol': kafka_cfg['security_protocol'],
        'sasl.username': kafka_cfg['sasl_username'],
        'sasl.password': kafka_cfg['sasl_password'],
        'key.serializer': key_avro_serializer,
        'value.serializer': value_avro_serializer,
      'group.id': 'cgrp1',
      'auto.offset.reset': 'earliest',
      # 'enable.auto.commit': True,      # default value
      # 'auto.commit.interval.ms': 5000, # Commit every 5000 ms, i.e., every 5 seconds
      # 'max.poll.interval.ms': 300000,
    }
    try:
        producer = SerializingProducer(kafka_config)
        logger.info("Kafka connection has been successfully established!")
    except Exception as err:
        logger.error(f"Kafka cluster connection error: {err}")



    # Define the DeserializingConsumer
    try:
      consumer = DeserializingConsumer(kafka_config)
    except Exception as e:
      logger.error(f"Kafka connection failed: {e}")

    # Subscribe to the 'product_updates' topic
    consumer.subscribe([kafka_cfg["topic"]])


    while True:
      msg = consumer.poll(1.0)
      if msg is None:
        continue
      if msg.error():
        print('Consumer error: {}'.format(msg.error()))
        continue

      value = msg.value()
