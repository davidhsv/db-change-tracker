import logging
from .gui import App

# Configure basic console logging as well
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()] # Output to console
)

# Suppress verbose Kafka subscription logs to reduce log spam
logging.getLogger('kafka.consumer.subscription_state').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def run_gui():
    """Initializes and runs the main application GUI."""
    logger.info("Starting Database Change Tracker GUI...")
    app = App()
    app.mainloop()
    logger.info("Database Change Tracker GUI closed.")

if __name__ == "__main__":
    run_gui()