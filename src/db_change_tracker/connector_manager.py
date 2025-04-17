import time
import requests
import logging
from . import config

logger = logging.getLogger(__name__)

def wait_for_connect_ready(timeout_sec=120):
    """Waits for the Kafka Connect REST API to become available."""
    logger.info(f"Waiting for Kafka Connect REST API at {config.KAFKA_CONNECT_HEALTH_URL}...")
    start_time = time.time()
    while time.time() - start_time < timeout_sec:
        try:
            response = requests.get(config.KAFKA_CONNECT_HEALTH_URL, timeout=5)
            if response.status_code == 200:
                logger.info("Kafka Connect is ready.")
                return True
        except requests.ConnectionError:
            logger.debug("Kafka Connect not ready yet, retrying...")
        except requests.Timeout:
             logger.debug("Kafka Connect health check timed out, retrying...")
        except Exception as e:
             logger.error(f"Error checking Kafka Connect status: {e}")
        time.sleep(3)
    logger.error(f"Kafka Connect did not become ready within {timeout_sec} seconds.")
    return False

def delete_connector(name: str):
    """Deletes an existing connector."""
    url = f"{config.KAFKA_CONNECT_CONNECTORS_URL}{name}"
    try:
        response = requests.delete(url, timeout=10)
        if response.status_code == 204:
            logger.info(f"Successfully deleted connector '{name}'.")
            return True
        elif response.status_code == 404:
            logger.info(f"Connector '{name}' not found, skipping deletion.")
            return True # Not an error if it doesn't exist
        else:
            logger.error(f"Failed to delete connector '{name}'. Status: {response.status_code}, Response: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error sending DELETE request for connector '{name}': {e}")
        return False

def create_postgres_connector(host, port, user, password, dbname, db_server_name="postgres_db_server") -> bool:
    """Creates the Debezium PostgreSQL connector."""
    connector_config = {
        "name": config.POSTGRES_CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": "host.docker.internal",
            "database.port": str(port),
            "database.user": user,
            "database.password": password,
            "database.dbname": dbname,
            "database.server.name": db_server_name, # Logical name for the server/topic prefix
            "topic.prefix": db_server_name,         # Use server name as topic prefix
            "plugin.name": "pgoutput",            # Logical decoding plugin
            "slot.name": f"debezium_{db_server_name.replace('.', '_').lower()}", # Ensure valid slot name
            "snapshot.mode": "no_data",
            "slot.drop.on.stop": "true" # Clean up slot when connector stops cleanly
        }
    }

    logger.info(f"Attempting to create PostgreSQL connector '{config.POSTGRES_CONNECTOR_NAME}'...")
    try:
        response = requests.post(config.KAFKA_CONNECT_CONNECTORS_URL, json=connector_config, headers={'Content-Type': 'application/json'}, timeout=30)
        if response.status_code == 201:
            logger.info(f"Successfully created PostgreSQL connector '{config.POSTGRES_CONNECTOR_NAME}'.")
            return True
        elif response.status_code == 409:
             logger.warning(f"PostgreSQL connector '{config.POSTGRES_CONNECTOR_NAME}' already exists. Attempting update.")
             # Try updating instead (PUT request to connector's config endpoint)
             update_url = f"{config.KAFKA_CONNECT_CONNECTORS_URL}{config.POSTGRES_CONNECTOR_NAME}/config"
             update_response = requests.put(update_url, json=connector_config['config'], headers={'Content-Type': 'application/json'}, timeout=30)
             if update_response.status_code == 200:
                 logger.info(f"Successfully updated PostgreSQL connector '{config.POSTGRES_CONNECTOR_NAME}'.")
                 return True
             else:
                  logger.error(f"Failed to update PostgreSQL connector. Status: {update_response.status_code}, Response: {update_response.text}")
                  return False
        else:
            logger.error(f"Failed to create PostgreSQL connector. Status: {response.status_code}, Response: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error sending POST request for PostgreSQL connector: {e}")
        return False


def create_mysql_connector(db_host, db_port, db_user, db_password, db_server_id="6660", db_server_name="allocadia") -> bool:
    """Creates the Debezium MySQL connector."""
    # Note: MySQL connector requires a database.include.list or database.whitelist
    # For simplicity here, we assume all non-system tables. Add database.include.list if needed.
    connector_config = {
        "name": config.MYSQL_CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": "host.docker.internal",
            "database.port": str(db_port),
            "database.user": db_user,
            "database.password": db_password,
            "database.server.id": db_server_id, # Must be unique across your MySQL topology
            "database.server.name": db_server_name, # Logical name / topic prefix
            "topic.prefix": db_server_name,
            "schema.history.internal.kafka.topic": config.CONNECT_SCHEMA_HISTORY_TOPIC,
            "schema.history.internal.kafka.bootstrap.servers": "kafka:29092", # Internal listener
            "include.schema.changes": "true",
            "table.exclude.list": ".*QRTZ_.*,.*PLANNING_OUTBOX.*",
            "snapshot.mode": "no_data", # Or initial, schema_only etc.
        }
    }

    logger.info(f"Attempting to create MySQL connector '{config.MYSQL_CONNECTOR_NAME}'...")
    try:
        response = requests.post(config.KAFKA_CONNECT_CONNECTORS_URL, json=connector_config, headers={'Content-Type': 'application/json'}, timeout=30)
        if response.status_code == 201:
            logger.info(f"Successfully created MySQL connector '{config.MYSQL_CONNECTOR_NAME}'.")
            return True
        elif response.status_code == 409:
             logger.warning(f"MySQL connector '{config.MYSQL_CONNECTOR_NAME}' already exists. Attempting update.")
             update_url = f"{config.KAFKA_CONNECT_CONNECTORS_URL}{config.MYSQL_CONNECTOR_NAME}/config"
             update_response = requests.put(update_url, json=connector_config['config'], headers={'Content-Type': 'application/json'}, timeout=30)
             if update_response.status_code == 200:
                 logger.info(f"Successfully updated MySQL connector '{config.MYSQL_CONNECTOR_NAME}'.")
                 return True
             else:
                 logger.error(f"Failed to update MySQL connector. Status: {update_response.status_code}, Response: {update_response.text}")
                 return False
        else:
            logger.error(f"Failed to create MySQL connector. Status: {response.status_code}, Response: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error sending POST request for MySQL connector: {e}")
        return False

def check_connector_status(name: str) -> dict | None:
    """Checks the status of a specific connector."""
    url = f"{config.KAFKA_CONNECT_CONNECTORS_URL}{name}/status"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
             logger.warning(f"Connector '{name}' status check failed: Not Found (404)")
             return None
        else:
            logger.error(f"Failed to get status for connector '{name}'. Status: {response.status_code}, Response: {response.text}")
            return None # Indicate error or non-existence
    except requests.RequestException as e:
        logger.error(f"Error checking status for connector '{name}': {e}")
        return None