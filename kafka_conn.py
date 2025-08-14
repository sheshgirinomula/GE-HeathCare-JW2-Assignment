import logging
import json
from kafka import KafkaConsumer

from constants import KAFKA_TOPIC, KAFKA_BROKER
from s3_conn import download_s3_file, delete_s3_file
from db_conn import get_db_connection

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def persist_data(tenant_id, device_id, data):
    """Persists processed data to the tenant's database."""
    conn = None
    try:
        conn = get_db_connection(tenant_id)
        cursor = conn.cursor()
        # Example: Insert data into a tenant-specific table.
        # This is highly dependent on your data structure and tenant isolation strategy.
        # Please provide more inputs for table schema.
        cursor.execute(
            f"INSERT INTO tenant_{tenant_id}_data (device_id, processed_data) VALUES (%s, %s)",
            (device_id, json.dumps(data)),  # Store as JSON or a suitable format
        )
        conn.commit()
        logging.info(f"Data persisted for tenant {tenant_id}, device {device_id}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error persisting data for tenant {tenant_id}, device {device_id}: {e}")
        raise
    finally:
        if conn:
            conn.close()


# --- Processing logic (example: simple text processing) ---
def process_file_content(local_filepath):
    """Processes the content of the downloaded file."""
    try:
        with open(local_filepath, "r") as f:
            content = f.read()
            # Implement your tenant-specific business logic here.
            processed_data = {"original_content_length": len(content), "timestamp": "current_time"}
            return processed_data
    except Exception as e:
        logging.error(f"Error processing file {local_filepath}: {e}")
        raise

# --- Kafka consumer thread ---
def consume_kafka_events():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',  # Start from the beginning if no committed offsets
        enable_auto_commit=True,
        group_id='file-processor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        logging.info(f"Received Kafka message: {message.value}")
        try:
            event_data = message.value
            bucket_name = event_data["s3"]["bucket"]["name"]
            object_key = event_data["s3"]["object"]["key"]
            tenant_id = event_data["Records"][0]["s3"]["object"]["metadata"]["tenant_id"] # Assuming tenant_id is in S3 object metadata
            device_id = event_data["Records"][0]["s3"]["object"]["metadata"]["device_id"] # Assuming device_id is in S3 object metadata

            local_filepath = download_s3_file(bucket_name, object_key)
            processed_data = process_file_content(local_filepath)
            persist_data(tenant_id, device_id, processed_data)
            delete_s3_file(bucket_name, object_key)
            logging.info(f"Successfully processed file for tenant {tenant_id}, object {object_key}")
        except Exception as e:
            logging.error(f"Error processing Kafka event: {e}")
            # Implement failure notification (e.g., send to another Kafka topic, email, or logging)