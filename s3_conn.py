import logging
import boto3
from constants import S3_REGION

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Initialize AWS S3 client ---
s3_client = boto3.client("s3", region_name=S3_REGION)

# --- S3 functions ---
def download_s3_file(bucket_name, object_key):
    """Downloads a file from S3."""
    try:
        local_filename = f"/tmp/{object_key.split('/')[-1]}"  # Temporary storage
        s3_client.download_file(bucket_name, object_key, local_filename)
        logging.info(f"Downloaded file {object_key} from bucket {bucket_name}")
        return local_filename
    except Exception as e:
        logging.error(f"Error downloading file {object_key} from S3 bucket {bucket_name}: {e}")
        raise

def delete_s3_file(bucket_name, object_key):
    """Deletes a file from S3."""
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
        logging.info(f"Deleted file {object_key} from bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Error deleting file {object_key} from S3 bucket {bucket_name}: {e}")
        raise