from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer,AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from typing import Dict, List, Tuple
from pathlib import Path

import csv
import os
from datetime import datetime
import sys
import time # For polling timeout
import traceback
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d - %(funcName)s()] - %(message)s')

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# KAFKA_BROKER= 'localhost:9092,localhost:9093,localhost:9094'
#SCHEMA_REGISTRY_URL = 'http://localhost:8082'
CSV_DIR = Path('kafka2csv')
CSV_DIR.mkdir(exist_ok=True)
MAX_MESSAGES_PER_FILE = 1000 # To prevent excessively large CSVs
CONSUMER_GROUP_ID = 'avro_csv_exporter_group_' + datetime.now().strftime('%Y%m%d%H%M%S') # Unique group ID
CONSUMER_GROUP_BASE_ID = 'avro_csv_topic_exporter'
# --- List of Topics to Process ---
#com.mdsol.schema.ravecdc.fields
TOPICS_TO_PROCESS = os.getenv("KAFKA_TOPICS").split(",")
# --- End Global Configuration ---

def create_consumer_config(group_id):
    return {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,    # Manually commit offsets for reliability
        'session.timeout.ms': 60000,
        'max.poll.interval.ms': 300000 # Increase if processing takes long
    }

def write_batch_to_csv(batch_messages, current_filename, header_written, fieldnames):
    try:
        with open(current_filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not header_written:
                writer.writeheader()
                header_written = True
            writer.writerows(batch_messages)
    except IOError as e:
        print(f"Error writing to CSV file {current_filename}: {e}")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    return header_written

TIMEOUT_SECONDS = 30
def get_topic_offsets(consumer_client: Consumer, topic_name: str) -> Dict[int, Tuple[int, int]]:
    """
    Gets the low (earliest) and high (latest) offsets for all partitions of a given topic.
    Returns a dictionary mapping partition ID to (low_offset, high_offset).
    """
    try:
        topic_metadata = consumer_client.list_topics(topic=topic_name, timeout=TIMEOUT_SECONDS)
        if topic_metadata.topics[topic_name].error:
            raise KafkaException(f"Failed to get metadata for topic {topic_name}: {topic_metadata.topics[topic_name].error.str()}")

        partitions_info = topic_metadata.topics[topic_name].partitions
        partition_offsets = {}
        for p_id in partitions_info:
            tp = TopicPartition(topic_name, p_id)
            low_offset, high_offset = consumer_client.get_watermark_offsets(tp, timeout=TIMEOUT_SECONDS)
            partition_offsets[p_id] = (low_offset, high_offset)
        return partition_offsets
    except KafkaException as e:
        print(f"Error fetching topic offsets for {topic_name}: {e}")
        traceback.print_exc(file=sys.stderr) # Print stack trace for KafkaException
        return {}


def initialize_consumer(topic_name):
    """
    Initializes the Kafka consumer and assigns partitions.
    """
    consumer_group_id = f"{CONSUMER_GROUP_BASE_ID}_{topic_name.replace('.', '_')}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    consumer_conf = create_consumer_config(consumer_group_id)
    consumer = Consumer(consumer_conf)

    initial_partition_offsets = get_topic_offsets(consumer, topic_name)
    if not initial_partition_offsets:
        logging.error(f"Could not get initial offsets for {topic_name}. Skipping this topic.")
        consumer.close()
        return None, None

    partition_high_watermarks = {
        p_id: offsets[1] for p_id, offsets in initial_partition_offsets.items()
    }

    consumer.subscribe([topic_name])
    assign_partitions(consumer, initial_partition_offsets)

    return consumer, partition_high_watermarks


def assign_partitions(consumer, initial_partition_offsets):
    """
    Assigns partitions to the consumer based on initial offsets.
    """
    assigned = False
    start_time = time.time()
    timeout_seconds = 30

    while not assigned and (time.time() - start_time) < timeout_seconds:
        consumer.poll(10)
        assigned_partitions = consumer.assignment()
        if assigned_partitions:
            partitions_to_assign = []
            for tp in assigned_partitions:
                if tp.partition in initial_partition_offsets:
                    tp.offset = initial_partition_offsets[tp.partition][0]
                    partitions_to_assign.append(tp)
            if partitions_to_assign:
                consumer.assign(partitions_to_assign)
                assigned = True
        else:
            time.sleep(0.5)

    if not assigned:
        logging.error(f"Failed to assign partitions within {timeout_seconds} seconds.")


def process_message(msg, value_serde, topic_name):
    """
    Processes a single Kafka message and deserializes its value.
    """
    if msg.error():
        logging.error(f"Kafka consumer error: {msg.error()}")
        return None

    try:
        data = value_serde(msg.value(), SerializationContext(topic_name, MessageField.VALUE))
        return data
    except Exception as e:
        logging.error(f"Error deserializing message: {e}")
        return None

def write_to_csv(messages, filename, header_written):
    if not messages:
        return header_written

    fieldnames = []
    if messages:
        fieldnames = list(messages[0].keys())

    try:
        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not header_written:
                writer.writeheader()
                header_written = True
            writer.writerows(messages)
    except IOError as e:
        print(f"Error writing to CSV file {filename}: {e}")
        traceback.print_exc(file=sys.stderr) # Print stack trace for IOError
        sys.exit(1)
    return header_written
def write_batch_to_file(batch_messages, base_csv_filename, current_filename, header_written, messages_in_current_file):
    """
    Writes a batch of messages to a CSV file.
    """
    if not batch_messages:
        return current_filename, header_written, messages_in_current_file

    if current_filename is None or messages_in_current_file >= MAX_MESSAGES_PER_FILE:
        timestamp_prefix = datetime.now().strftime('%Y%m%d_%H%M%S_')
        current_filename = os.path.join(CSV_DIR, f'{timestamp_prefix}{base_csv_filename}')
        messages_in_current_file = 0
        header_written = False

    header_written = write_to_csv(batch_messages, current_filename, header_written)
    messages_in_current_file += len(batch_messages)
    return current_filename, header_written, messages_in_current_file


def consume_and_write_csv_avro(topic_name):
    """
    Main function to consume Kafka messages and write them to a CSV file.
    """
    logging.info(f"Starting consumption for topic: {topic_name}")
    base_csv_filename = topic_name.split('.')[-1] + '.csv'

    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    value_schema_str = schema_registry_client.get_latest_version(f"{topic_name}-value").schema.schema_str
    value_serde = AvroDeserializer(schema_registry_client, value_schema_str)

    consumer, partition_high_watermarks = initialize_consumer(topic_name)
    logging.info(f"Initialized consumer for topic {topic_name} with group ID {consumer.group_id()}, partition high watermarks: {partition_high_watermarks}")
    if not consumer:
        return

    os.makedirs(CSV_DIR, exist_ok=True)
    current_filename = None
    messages_in_current_file = 0
    header_written = False
    batch_messages = []
    messages_consumed_overall = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                current_filename, header_written, messages_in_current_file = write_batch_to_file(
                    batch_messages, base_csv_filename, current_filename, header_written, messages_in_current_file
                )
                batch_messages = []
                if all_partitions_consumed(consumer, partition_high_watermarks):
                    break
                continue

            data = process_message(msg, value_serde, topic_name)
            if data:
                batch_messages.append(data)
                messages_consumed_overall += 1

            if len(batch_messages) >= 100 or messages_in_current_file + len(batch_messages) >= MAX_MESSAGES_PER_FILE:
                logging.info(f"Writing batch of {len(batch_messages)} messages to file {current_filename or base_csv_filename}")
                current_filename, header_written, messages_in_current_file = write_batch_to_file(
                    batch_messages, base_csv_filename, current_filename, header_written, messages_in_current_file
                )
                batch_messages = []

    except KeyboardInterrupt:
        logging.info("Stopping consumption due to user interrupt.")
    finally:
        consumer.close()
        logging.info("Kafka consumer closed.")


def all_partitions_consumed(consumer, partition_high_watermarks):
    """
    Checks if all partitions have been consumed.
    """
    for tp in consumer.assignment():
        current_position = consumer.position([tp])[0].offset
        high_watermark = partition_high_watermarks.get(tp.partition)
        logging.info(f"tp={tp}, current_position={current_position}, high_watermark={high_watermark}")
        # If the high watermark is 0, consider the partition consumed
        if high_watermark == 0:
            continue
        if high_watermark is None or current_position < high_watermark:
            return False
    return True

if __name__ == "__main__":
    for topic in TOPICS_TO_PROCESS:
        consume_and_write_csv_avro(topic)
    print("\n--- All topics processed. ---")