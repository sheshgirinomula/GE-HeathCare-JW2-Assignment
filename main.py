import logging
from fastapi import FastAPI, HTTPException

from db_conn import get_db_connection
from kafka_conn import consume_kafka_events

# --- Initialize FastAPI app ---
app = FastAPI()

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# --- REST API endpoints ---
@app.get("/data/{tenant_id}/{device_id}")
async def get_ingested_data(tenant_id: str, device_id: str):
    """Retrieves ingested data based on tenant_id and device_id."""
    conn = None
    try:
        conn = get_db_connection(tenant_id)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT processed_data FROM tenant_{tenant_id}_data WHERE device_id = %s", (device_id,)
        )
        result = cursor.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Data not found")
        return {"tenant_id": tenant_id, "device_id": device_id, "data": [row[0] for row in result]}
    except Exception as e:
        logging.error(f"Error retrieving data for tenant {tenant_id}, device {device_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")
    finally:
        if conn:
            conn.close()

# --- Main execution ---
if __name__ == "__main__":
    import threading
    import uvicorn

    # Start Kafka consumer in a separate thread
    consumer_thread = threading.Thread(target=consume_kafka_events)
    consumer_thread.start()

    # Start FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=8000)