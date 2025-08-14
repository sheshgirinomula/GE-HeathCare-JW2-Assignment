# GE-HeathCare-JW2-Assignment
Develop a multi-tenant application that listens to Kafka events triggered when a file is uploaded to an S3 bucket. The application should handle tenant-specific processing and data persistence.
# Functional Requirements:
 Event-Driven File Processing:
 The application should consume Kafka events that indicate a new file has been added to an S3 bucket.
 Each event corresponds to a tenant-specific file upload.
# Tenant-Specific File Handling:
 Retrieve the uploaded file from the appropriate tenants S3 bucket.
 Process the file content according to business logic.
 Persist the extracted data into the corresponding tenant-specific database.
# REST API Exposure:
 Provide RESTful endpoints to retrieve the ingested data.
 API inputs should include tenant_id and device_id to filter results.
# Post-Processing Cleanup:
 Delete the file from S3 after successful processing.
 Implement robust error handling for scenarios such as:
 Failures during file retrieval or processing.
 Issues during database insertion.
# Failure Notifications (Optional):
 Publish alerts or notifications in case of processing failures to enable timely intervention.

# Explanation and features
1.Kafka Consumer (consume_kafka_events):
-->Connects to the specified Kafka topic and consumes messages.
-->Parses the Kafka event (assuming an S3 event notification format with tenant_id in S3 object metadata).
-->Tenant-Specific Processing: Retrieves the tenant_id and device_id from the event.
-->Downloads the corresponding file from S3 using boto3.download_file().
-->Processes the file content (demonstrated with a basic example; this is where your tenant-specific business logic goes).
-->Data Persistence: Persists the processed data into the tenant's dedicated database (indicated by tenant_id in the table name in this example).
-->Cleanup: Deletes the file from the S3 bucket using boto3.delete_object() after successful processing.
-->Includes basic error handling for file download, processing, and database operations.
2.REST API (/data/{tenant_id}/{device_id}):
-->Exposes a GET endpoint using FastAPI to retrieve ingested data filtered by tenant_id and device_id.
-->Connects to the database (using tenant_id to infer the correct table) and retrieves the relevant data.
-->Returns the data as a JSON response.
-->Includes error handling and returns appropriate HTTP status codes (e.g., 404 if data not found, 500 for internal errors).
3.Multi-Tenancy Considerations:
-->The code leverages tenant_id embedded in Kafka messages and S3 object metadata to handle tenant-specific data and processing.
-->This example assumes a tenant-specific database schema (e.g., tenant_X_data tables), but you could also implement other multi-tenancy models like a shared schema with tenant IDs as a column.
4.Error Handling:
-->Uses try-except blocks to handle potential errors during S3 interactions, file processing, and database operations.
-->Logs detailed error messages using the logging module.
-->Database transactions are rolled back in case of database failures.

Note:-
Ensure Kafka and S3 setup: Make sure your Kafka cluster is running and S3 event notifications are configured to send events to the specified Kafka topic.
Update the KAFKA_BROKER, KAFKA_TOPIC, S3_REGION, and DB_CONFIG variables in constants.py file.
first check if python installed in machine by running python --version if it return Python 3.10.0 or any values, means python is installed, if not install python 3.10 or higher
run pip install -r requirements.txt ---> It will install all the python modules required for the project, then run
python main.py
