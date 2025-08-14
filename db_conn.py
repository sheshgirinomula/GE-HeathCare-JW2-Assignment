import psycopg2 # Use the appropriate database library
from constants import DB_CONFIG
import logging

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- Database functions (placeholder for tenant-specific logic) ---
def get_db_connection(tenant_id):
    """Establishes and returns a database connection for a given tenant."""
    # In a real-world scenario, you'd fetch tenant-specific DB credentials or connect to a pooled connection.
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database for tenant {tenant_id}: {e}")
        raise