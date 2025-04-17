import os

# --- Docker Configuration ---
DOCKER_NETWORK_NAME = "db-change-tracker-net"
ZOOKEEPER_IMAGE = "quay.io/debezium/zookeeper:3.0"
KAFKA_IMAGE = "quay.io/debezium/kafka:3.0"
KAFKA_CONNECT_IMAGE = "debezium/connect:3.0.0.Final" # Using official name

ZOOKEEPER_CONTAINER_NAME = "zookeeper-dbtracker"
KAFKA_CONTAINER_NAME = "kafka-dbtracker"
KAFKA_CONNECT_CONTAINER_NAME = "kafka-connect-dbtracker"

KAFKA_EXTERNAL_PORT = 39092
KAFKA_CONNECT_REST_PORT = 8083

KAFKA_CONNECT_HEALTH_URL = f"http://localhost:{KAFKA_CONNECT_REST_PORT}/"
KAFKA_CONNECT_CONNECTORS_URL = f"http://localhost:{KAFKA_CONNECT_REST_PORT}/connectors/"

# --- Debezium Connector Configuration ---
POSTGRES_CONNECTOR_NAME = "dbtracker-postgres-connector"
MYSQL_CONNECTOR_NAME = "dbtracker-mysql-connector"
CONNECT_CONFIG_STORAGE_TOPIC = "dbtracker_connect_configs"
CONNECT_OFFSET_STORAGE_TOPIC = "dbtracker_connect_offsets"
CONNECT_STATUS_STORAGE_TOPIC = "dbtracker_connect_statuses"
CONNECT_SCHEMA_HISTORY_TOPIC = "schema-changes.allocadia" # Specific to MySQL

# --- Consumer Configuration ---
CONSUMER_GROUP_ID = "db-change-tracker-consumer-v1"
CONSUMER_BOOTSTRAP_SERVERS = f"localhost:{KAFKA_EXTERNAL_PORT}"
CONSUMER_TOPIC_PATTERN = '.*\..*\..*' # Matches topic format server.schema.table

# --- Database Default Ports ---
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_MYSQL_PORT = 3306

# --- Default User/Password (Example - User provides these via GUI) ---
# These are just illustrative, GUI input overrides them
DEFAULT_PG_USER = "postgres"
DEFAULT_PG_PASS = "password"
DEFAULT_MYSQL_USER = "root"
DEFAULT_MYSQL_PASS = "allocadia1!"

# --- GUI ---
APP_TITLE = "Database Change Tracker"