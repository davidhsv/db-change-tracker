import time
import logging
from python_on_whales import DockerClient, DockerException
from python_on_whales.components.container.cli_wrapper import Container
from . import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

docker = None

# --- Helper Function ---
def _ensure_container_removed(client: DockerClient, container_name: str):
    """Checks if a container exists and attempts to stop and remove it."""
    try:
        container = client.container.inspect(container_name)
        if container:
            logger.warning(f"Container '{container_name}' already exists (ID: {container.id[:12]}, Status: {container.state.status}). Attempting removal...")
            try:
                # Force stop might be necessary if it's stuck
                logger.info(f"Stopping existing container '{container_name}'...")
                client.container.stop(container, time=5) # Give 5 seconds to stop gracefully
            except DockerException as stop_err:
                 # Ignore errors if it's already stopped or gone, log others
                 if "No such container" not in str(stop_err) and "is not running" not in str(stop_err):
                      logger.warning(f"Issue stopping existing container '{container_name}', attempting force remove anyway: {stop_err}")
                 # Else, container likely already stopped or gone

            try:
                logger.info(f"Removing existing container '{container_name}'...")
                # Force remove it
                client.container.remove(container, force=True, volumes=False) # Don't remove associated volumes if any defined externally
                logger.info(f"Successfully removed existing container '{container_name}'.")
                time.sleep(1) # Brief pause after removal
            except DockerException as remove_err:
                logger.error(f"Failed to remove existing container '{container_name}': {remove_err}")
                raise # Re-raise the error as we cannot proceed safely

    except DockerException as e:
        # This is expected if the container doesn't exist
        if "No such container" in str(e) or "not found" in str(e):
            logger.debug(f"Container '{container_name}' does not exist, proceeding.")
        else:
            # Log and re-raise other unexpected errors during inspection
            logger.error(f"Error checking for potentially conflicting container '{container_name}': {e}")
            raise

def get_docker_client() -> DockerClient:
    """Initializes and returns the Docker client."""
    global docker
    if docker is None:
        try:
            # The DockerClient() constructor or a subsequent call like info()
            # will raise DockerException if the daemon is not running.
            docker = DockerClient()
            # Perform a simple operation to confirm connectivity after init
            docker.info()
            logger.info("Docker client initialized and connected successfully.")
        except DockerException as e:
            logger.error(f"Failed to initialize or connect Docker client: {e}")
            # Ensure docker remains None if initialization failed
            docker = None
            raise # Re-raise the exception to be caught by the caller
        except Exception as e:
            logger.error(f"Unexpected error initializing Docker client: {e}", exc_info=True)
            docker = None
            raise
    return docker

def start_services() -> bool:
    """Starts Zookeeper, Kafka, and Kafka Connect containers."""
    
    client = None # Initialize client to None
    try:
        client = get_docker_client()
        logger.info("Starting required Docker services...")
        
        _ensure_container_removed(client, config.KAFKA_CONNECT_CONTAINER_NAME)
        _ensure_container_removed(client, config.KAFKA_CONTAINER_NAME)
        _ensure_container_removed(client, config.ZOOKEEPER_CONTAINER_NAME)

        # Create network if not exists
        try:
            network = client.network.inspect(config.DOCKER_NETWORK_NAME)
            logger.info(f"Using existing Docker network: {network.name}")
        except DockerException:
            logger.info(f"Creating Docker network: {config.DOCKER_NETWORK_NAME}")
            network = client.network.create(config.DOCKER_NETWORK_NAME, driver="bridge")
            logger.info(f"Created Docker network: {network.name}")


        # Start Zookeeper
        logger.info(f"Starting Zookeeper ({config.ZOOKEEPER_IMAGE})...")
        zookeeper = client.container.run(
            config.ZOOKEEPER_IMAGE,
            detach=True,
            name=config.ZOOKEEPER_CONTAINER_NAME,
            networks=[network.name],
            hostname="zookeeper",
            envs={"ALLOW_ANONYMOUS_LOGIN": "yes"},
            remove=True, # Remove container on stop
            pull="missing", # Pull if image not present
        )
        logger.info(f"Zookeeper container started: {zookeeper.id[:12]}")
        time.sleep(2) # Brief pause for zookeeper

        # Start Kafka
        logger.info(f"Starting Kafka ({config.KAFKA_IMAGE})...")
        kafka = client.container.run(
            config.KAFKA_IMAGE,
            detach=True,
            name=config.KAFKA_CONTAINER_NAME,
            networks=[network.name],
            hostname="kafka",
            publish=[(config.KAFKA_EXTERNAL_PORT, 39092)],
            envs={
                "KAFKA_LISTENERS": "INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:39092",
                "KAFKA_ADVERTISED_LISTENERS": "INTERNAL://kafka:29092,EXTERNAL://localhost:39092",
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
                "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
                "ZOOKEEPER_CONNECT": "zookeeper:2181",
                "KAFKA_BROKER_ID": "1",
                "ALLOW_PLAINTEXT_LISTENER": "yes",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1", # For single node cluster
                "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0", # Speed up startup
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
                "KAFKA_CONFIG_STORAGE_REPLICATION_FACTOR": "1", # Added based on connect needs
                "KAFKA_OFFSET_STORAGE_REPLICATION_FACTOR": "1", # Added based on connect needs
                "KAFKA_STATUS_STORAGE_REPLICATION_FACTOR": "1", # Added based on connect needs
            },
            # remove dependencies, rely on service readiness checks instead
            remove=True,
            pull="missing",
        )
        logger.info(f"Kafka container started: {kafka.id[:12]}")
        # Basic wait for Kafka, more robust check needed if issues arise
        logger.info("Waiting a few seconds for Kafka to initialize...")
        time.sleep(10)

        # Start Kafka Connect
        logger.info(f"Starting Kafka Connect ({config.KAFKA_CONNECT_IMAGE})...")
        connect = client.container.run(
            config.KAFKA_CONNECT_IMAGE,
            detach=True,
            name=config.KAFKA_CONNECT_CONTAINER_NAME,
            networks=[network.name],
            hostname="kafka-connect",
            publish=[(config.KAFKA_CONNECT_REST_PORT, 8083)],
            envs={
                "BOOTSTRAP_SERVERS": "kafka:29092", # Internal listener
                "CONFIG_STORAGE_TOPIC": config.CONNECT_CONFIG_STORAGE_TOPIC,
                "OFFSET_STORAGE_TOPIC": config.CONNECT_OFFSET_STORAGE_TOPIC,
                "STATUS_STORAGE_TOPIC": config.CONNECT_STATUS_STORAGE_TOPIC,
                # Set replication factor for internal topics Kafka Connect creates
                "CONFIG_STORAGE_REPLICATION_FACTOR": "1",
                "OFFSET_STORAGE_REPLICATION_FACTOR": "1",
                "STATUS_STORAGE_REPLICATION_FACTOR": "1",
                "KEY_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
                "VALUE_CONVERTER": "org.apache.kafka.connect.json.JsonConverter",
                "KEY_CONVERTER_SCHEMAS_ENABLE": "false",
                "VALUE_CONVERTER_SCHEMAS_ENABLE": "false",
                "CONNECT_REST_ADVERTISED_HOST_NAME": "localhost", # For accessing REST API from host
                "CONNECT_LOG4J_ROOT_LOGLEVEL": "INFO"
            },
            # remove dependencies
            remove=True,
            pull="missing",
        )
        logger.info(f"Kafka Connect container started: {connect.id[:12]}")
        logger.info("Docker services started successfully.")
        return True

    # Catch DockerException specifically from get_docker_client or run calls
    except DockerException as e:
        logger.error(f"Docker error during service startup: {e}")
        # Attempt cleanup if client was initialized before error
        if client:
             stop_services()
        return False
    except Exception as e:
        logger.error(f"Unexpected error starting Docker services: {e}", exc_info=True)
        # Attempt cleanup if client was initialized before error
        if client:
             stop_services()
        return False


def stop_services():
    """Stops and removes the managed Docker containers and network."""
    client = None
    try:
        # Use get_docker_client to ensure we have a valid client if possible
        # It might raise an exception if docker daemon is down, which is okay here.
        client = get_docker_client()
        logger.info("Stopping Docker services...")

        containers_to_stop = [
            config.KAFKA_CONNECT_CONTAINER_NAME,
            config.KAFKA_CONTAINER_NAME,
            config.ZOOKEEPER_CONTAINER_NAME,
        ]

        stopped_count = 0
        # Stop containers individually and ignore "not found" errors
        for container_name in containers_to_stop:
            try:
                logger.debug(f"Attempting to stop container: {container_name}")
                # Stop takes container name(s) as positional args
                client.container.stop(container_name, time=5) # Add a short timeout
                logger.info(f"Stopped container: {container_name}")
                stopped_count += 1
            except DockerException as e:
                # Check if the error indicates the container was not found
                # Error message might vary slightly depending on Docker version / OS
                if "No such container" in str(e) or "is not running" in str(e):
                    logger.info(f"Container '{container_name}' not found or not running, skipping stop.")
                else:
                    # Log other Docker errors during stop
                    logger.error(f"Error stopping container '{container_name}': {e}")

        logger.info(f"Finished stopping containers attempt. Stopped {stopped_count} container(s).")

        # Removal is handled by remove=True in run command, but ensure they are gone if stop failed
        # This part might not be strictly necessary with remove=True but adds robustness
        for name in containers_to_stop:
             try:
                 c = client.container.inspect(name)
                 # Check if it exists but failed to stop
                 if c and (c.state.running or c.state.paused):
                     logger.warning(f"Container {name} still exists after stop attempt. Force removing.")
                     client.container.remove(name, force=True, volumes=False) # Don't remove volumes if any were mounted externally
             except DockerException as e:
                  if "No such container" in str(e):
                      pass # Expected if already removed or never created
                  else:
                       logger.warning(f"Issue inspecting/removing container {name} after stop: {e}")


        # Remove network if exists
        try:
            network = client.network.inspect(config.DOCKER_NETWORK_NAME)
            containers_in_net = network.containers
            if containers_in_net:
                logger.warning(f"Network {config.DOCKER_NETWORK_NAME} still has containers attached: {[c.name for c in containers_in_net]}. Detaching...")
                for container_obj in containers_in_net:
                    try:
                         client.network.disconnect(network.name, container_obj, force=True)
                    except DockerException as detach_err:
                        # Log but continue trying to remove network
                        logger.error(f"Could not detach container {container_obj.name} from network: {detach_err}")

            logger.info(f"Removing network: {config.DOCKER_NETWORK_NAME}")
            client.network.remove(config.DOCKER_NETWORK_NAME)
            logger.info(f"Removed network: {config.DOCKER_NETWORK_NAME}")
        except DockerException as e:
            if "No such network" in str(e) or "not found" in str(e):
                 logger.info(f"Network {config.DOCKER_NETWORK_NAME} not found, skipping removal.")
            else:
                logger.error(f"Error removing network {config.DOCKER_NETWORK_NAME}: {e}")

        logger.info("Docker services cleanup process finished.")

    except DockerException as e:
        # This might happen if get_docker_client fails (e.g., daemon stopped between start and stop)
        logger.error(f"Docker error during service stop/cleanup: {e}")
    except Exception as e:
         logger.error(f"Unexpected error stopping Docker services: {e}", exc_info=True)

def is_container_running(container_name: str) -> bool:
    """Checks if a container with the given name is running."""
    try:
        client = get_docker_client()
        container = client.container.inspect(container_name)
        return container.state.running
    except DockerException:
        return False
    except Exception as e:
         logger.error(f"Error checking container status for {container_name}: {e}")
         return False # Treat error as not running