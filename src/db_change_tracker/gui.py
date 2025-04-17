import tkinter as tk
import customtkinter as ctk
from customtkinter import CTkTabview # Import CTkTabview
import threading
import logging
import time
import queue # Import queue for safer cross-thread communication

from . import config, docker_manager, connector_manager, db_initializer, consumer

logger = logging.getLogger(__name__)

ctk.set_appearance_mode("System")
ctk.set_default_color_theme("blue")

# Define color mappings for Tkinter tags
# Use names that match the COLOR_* constants conceptually
# Tkinter standard colors: https://www.tcl.tk/man/tcl8.6/TkCmd/colors.htm
# Or use hex codes
TAG_COLORS = {
    "header": {"foreground": "#00FFFF"}, # Cyan-ish
    "op_insert": {"foreground": "#00FF00"}, # Bright Green
    "op_update": {"foreground": "#FFFF00"}, # Bright Yellow
    "op_delete": {"foreground": "#FF0000"}, # Bright Red
    "op_read": {"foreground": "#87CEFA"}, # LightSkyBlue (for Read)
    "op_unknown": {"foreground": "#FF00FF"}, # Magenta
    "section": {"foreground": "#FF00FF"}, # Magenta
    "field_name": {"foreground": "#6495ED"}, # CornflowerBlue
    "before": {"foreground": "#FF4500"}, # OrangeRed (more distinct than pure red)
    "after": {"foreground": "#32CD32"}, # LimeGreen
    "unchanged": {"foreground": "#A9A9A9"}, # DarkGray
    "timestamp": {"foreground": "#FFA500"}, # Orange
    "separator": {"foreground": "#D3D3D3"}, # LightGray
    "error": {"foreground": "#FF0000"}, # Red Bold
    "normal": {"foreground": "#FFFFFF"}, # Default text color
}


class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title(config.APP_TITLE)
        self.geometry(f"{900}x{800}") # Slightly wider/taller for tabs

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1) # Make tab view expand

        self.running_state = False
        self.consumer_thread = None
        self.stop_consumer_event = threading.Event()
        self.gui_queue = queue.Queue() # Queue for thread-safe GUI updates

        # --- Database Connection Frame (remains the same) ---
        self.db_frame = ctk.CTkFrame(self)
        self.db_frame.grid(row=0, column=0, padx=20, pady=(20, 10), sticky="nsew")
        # ... (all the entry fields and labels for DB connections - no changes here) ...
        self.db_frame.grid_columnconfigure(1, weight=1)
        self.db_frame.grid_columnconfigure(3, weight=1)

        # --- Database Selection Checkboxes ---
        self.enable_pg_var = tk.BooleanVar(value=True)
        self.enable_pg_checkbox = ctk.CTkCheckBox(
            self.db_frame, text="Enable PostgreSQL", variable=self.enable_pg_var,
            command=self.toggle_pg_fields
        )
        self.enable_pg_checkbox.grid(row=0, column=0, columnspan=2, padx=10, pady=(10, 5), sticky="w")
        self.enable_mysql_var = tk.BooleanVar(value=True)
        self.enable_mysql_checkbox = ctk.CTkCheckBox(
            self.db_frame, text="Enable MySQL", variable=self.enable_mysql_var,
            command=self.toggle_mysql_fields
        )
        self.enable_mysql_checkbox.grid(row=0, column=2, columnspan=2, padx=10, pady=(10, 5), sticky="w")

        # PostgreSQL Section
        self.pg_label = ctk.CTkLabel(self.db_frame, text="PostgreSQL Connection", font=ctk.CTkFont(size=14, weight="bold"))
        self.pg_label.grid(row=1, column=0, columnspan=2, padx=10, pady=(10, 5), sticky="w")
        self.pg_host_label = ctk.CTkLabel(self.db_frame, text="Host:")
        self.pg_host_label.grid(row=2, column=0, padx=(20, 5), pady=5, sticky="w")
        self.pg_host_entry = ctk.CTkEntry(self.db_frame, placeholder_text="localhost")
        self.pg_host_entry.grid(row=2, column=1, padx=(0, 20), pady=5, sticky="ew")
        self.pg_host_entry.insert(0, "localhost")
        self.pg_port_label = ctk.CTkLabel(self.db_frame, text="Port:")
        self.pg_port_label.grid(row=3, column=0, padx=(20, 5), pady=5, sticky="w")
        self.pg_port_entry = ctk.CTkEntry(self.db_frame, placeholder_text=str(config.DEFAULT_POSTGRES_PORT))
        self.pg_port_entry.grid(row=3, column=1, padx=(0, 20), pady=5, sticky="ew")
        self.pg_port_entry.insert(0, str(config.DEFAULT_POSTGRES_PORT))
        self.pg_user_label = ctk.CTkLabel(self.db_frame, text="Debezium User:")
        self.pg_user_label.grid(row=4, column=0, padx=(20, 5), pady=5, sticky="w")
        self.pg_user_entry = ctk.CTkEntry(self.db_frame, placeholder_text="debezium_user")
        self.pg_user_entry.grid(row=4, column=1, padx=(0, 20), pady=5, sticky="ew")
        self.pg_user_entry.insert(0, "provisioning")
        self.pg_pass_label = ctk.CTkLabel(self.db_frame, text="Password:")
        self.pg_pass_label.grid(row=5, column=0, padx=(20, 5), pady=5, sticky="w")
        self.pg_pass_entry = ctk.CTkEntry(self.db_frame, show="*")
        self.pg_pass_entry.grid(row=5, column=1, padx=(0, 20), pady=5, sticky="ew")
        self.pg_pass_entry.insert(0, "provisioning")
        self.pg_dbname_label = ctk.CTkLabel(self.db_frame, text="Database Name:")
        self.pg_dbname_label.grid(row=6, column=0, padx=(20, 5), pady=5, sticky="w")
        self.pg_dbname_entry = ctk.CTkEntry(self.db_frame, placeholder_text="mydatabase")
        self.pg_dbname_entry.grid(row=6, column=1, padx=(0, 20), pady=5, sticky="ew")
        self.pg_dbname_entry.insert(0, "local__local_18")

        # MySQL Section
        self.mysql_label = ctk.CTkLabel(self.db_frame, text="MySQL Connection", font=ctk.CTkFont(size=14, weight="bold"))
        self.mysql_label.grid(row=1, column=2, columnspan=2, padx=10, pady=(10, 5), sticky="w")
        self.mysql_host_label = ctk.CTkLabel(self.db_frame, text="Host:")
        self.mysql_host_label.grid(row=2, column=2, padx=(20, 5), pady=5, sticky="w")
        self.mysql_host_entry = ctk.CTkEntry(self.db_frame, placeholder_text="localhost")
        self.mysql_host_entry.grid(row=2, column=3, padx=(0, 20), pady=5, sticky="ew")
        self.mysql_host_entry.insert(0, "localhost")
        self.mysql_port_label = ctk.CTkLabel(self.db_frame, text="Port:")
        self.mysql_port_label.grid(row=3, column=2, padx=(20, 5), pady=5, sticky="w")
        self.mysql_port_entry = ctk.CTkEntry(self.db_frame, placeholder_text=str(config.DEFAULT_MYSQL_PORT))
        self.mysql_port_entry.grid(row=3, column=3, padx=(0, 20), pady=5, sticky="ew")
        self.mysql_port_entry.insert(0, str(config.DEFAULT_MYSQL_PORT))
        self.mysql_dbz_user_label = ctk.CTkLabel(self.db_frame, text="Debezium User:")
        self.mysql_dbz_user_label.grid(row=4, column=2, padx=(20, 5), pady=5, sticky="w")
        self.mysql_dbz_user_entry = ctk.CTkEntry(self.db_frame, placeholder_text="debezium_user")
        self.mysql_dbz_user_entry.grid(row=4, column=3, padx=(0, 20), pady=5, sticky="ew")
        self.mysql_dbz_user_entry.insert(0, "v2user")
        self.mysql_dbz_pass_label = ctk.CTkLabel(self.db_frame, text="Password:")
        self.mysql_dbz_pass_label.grid(row=5, column=2, padx=(20, 5), pady=5, sticky="w")
        self.mysql_dbz_pass_entry = ctk.CTkEntry(self.db_frame, show="*")
        self.mysql_dbz_pass_entry.grid(row=5, column=3, padx=(0, 20), pady=5, sticky="ew")
        self.mysql_dbz_pass_entry.insert(0, "k4klw44")
        self.mysql_admin_user_label = ctk.CTkLabel(self.db_frame, text="Admin User:")
        self.mysql_admin_user_label.grid(row=6, column=2, padx=(20, 5), pady=5, sticky="w")
        self.mysql_admin_user_entry = ctk.CTkEntry(self.db_frame, placeholder_text="root")
        self.mysql_admin_user_entry.grid(row=6, column=3, padx=(0, 20), pady=5, sticky="ew")
        self.mysql_admin_user_entry.insert(0, "root")
        self.mysql_admin_pass_label = ctk.CTkLabel(self.db_frame, text="Admin Pwd:")
        self.mysql_admin_pass_label.grid(row=7, column=2, padx=(20, 5), pady=5, sticky="w")
        self.mysql_admin_pass_entry = ctk.CTkEntry(self.db_frame, show="*", placeholder_text="admin password")
        self.mysql_admin_pass_entry.grid(row=7, column=3, padx=(0, 20), pady=5, sticky="ew")
        self.mysql_admin_pass_entry.insert(0, "allocadia1!")

        # --- Control Frame (Add Clear Button) ---
        self.control_frame = ctk.CTkFrame(self)
        self.control_frame.grid(row=1, column=0, padx=20, pady=10, sticky="nsew")
        # Adjust column configuration for three buttons
        self.control_frame.grid_columnconfigure(0, weight=1)
        self.control_frame.grid_columnconfigure(1, weight=1)
        self.control_frame.grid_columnconfigure(2, weight=1) # Add weight for the third column
        
        self.start_button = ctk.CTkButton(self.control_frame, text="Start Tracking", command=self.start_tracking_thread)
        self.start_button.grid(row=0, column=0, padx=(20, 10), pady=10, sticky="ew") # Adjust padding

        self.stop_button = ctk.CTkButton(self.control_frame, text="Stop Tracking", command=self.stop_tracking, state=tk.DISABLED)
        self.stop_button.grid(row=0, column=1, padx=10, pady=10, sticky="ew") # Adjust padding

        # Add the Clear Button
        self.clear_button = ctk.CTkButton(self.control_frame, text="Clear Output", command=self.clear_output_tabs)
        self.clear_button.grid(row=0, column=2, padx=(10, 20), pady=10, sticky="ew") # Adjust padding

        # --- Tab View for Logs and Consumer Output ---
        self.tab_view = CTkTabview(self)
        self.tab_view.grid(row=2, column=0, padx=20, pady=(10, 20), sticky="nsew")
        self.tab_view.add("Logs")
        self.tab_view.add("Consumer Output")

        # --- Log Textbox (in Logs Tab) ---
        self.log_textbox = ctk.CTkTextbox(self.tab_view.tab("Logs"), state=tk.DISABLED, wrap=tk.WORD)
        self.log_textbox.pack(expand=True, fill="both", padx=5, pady=5)

        # --- Consumer Output Textbox (in Consumer Output Tab) ---
        self.consumer_output_textbox = ctk.CTkTextbox(self.tab_view.tab("Consumer Output"), state=tk.DISABLED, wrap=tk.WORD)
        self.consumer_output_textbox.pack(expand=True, fill="both", padx=5, pady=5)
        # Configure tags for colors in the consumer textbox
        self._configure_consumer_tags()

        # Redirect logging to the log textbox
        self.log_handler = GUILoggingHandler(self.log_textbox) # Pass the correct textbox
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        self.log_handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(self.log_handler)
        root_logger.setLevel(logging.INFO)

        # Ensure clean exit
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

        # Start polling the queue for GUI updates
        self.after(100, self.process_gui_queue)

    def _configure_consumer_tags(self):
        """Configure Tkinter tags for colored output in the consumer textbox."""
        for tag_name, config_dict in TAG_COLORS.items():
            # Use tk.NORMAL for base font if not specified
#             font_config = config_dict.get("font", tk.NORMAL)
            self.consumer_output_textbox.tag_config(
                tag_name,
                foreground=config_dict.get("foreground", TAG_COLORS["normal"]["foreground"]),
#                 font=font_config
                # Add other options like background, underline, etc. if needed
            )

    def queue_consumer_message(self, message_segment, style_tag="normal"):
        """
        Callback function passed to the consumer thread.
        Puts the message segment and its style tag into the GUI queue.
        """
        # Make sure style_tag exists in our defined tags
        safe_style_tag = style_tag if style_tag in TAG_COLORS else "normal"
        self.gui_queue.put(("consumer_output", message_segment, safe_style_tag))

    def process_gui_queue(self):
        """Processes messages from the queue in the main GUI thread."""
        try:
            while True: # Process all waiting messages
                message_type, *data = self.gui_queue.get_nowait()

                if message_type == "consumer_output":
                    message_segment, style_tag = data
                    self._append_consumer_text(message_segment, style_tag)
                # Add other message types if needed later

        except queue.Empty:
            pass # No more messages for now
        finally:
            # Reschedule itself to run again
            self.after(100, self.process_gui_queue)


    def _append_consumer_text(self, text, style_tag="normal"):
        """Appends text with a specific style tag to the consumer output textbox."""
        try:
            self.consumer_output_textbox.configure(state=tk.NORMAL)
            # Insert text with the specified tag
            self.consumer_output_textbox.insert(tk.END, text, (style_tag,))
            self.consumer_output_textbox.configure(state=tk.DISABLED)
            # Optional: Only scroll if the tab is visible? Might be complex. Scroll always for now.
            self.consumer_output_textbox.see(tk.END)
        except Exception as e:
            logger.error(f"Error updating consumer output GUI: {e}")

    def add_status_message(self, message, level="INFO"):
        """Appends a message to the LOG textbox."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"{timestamp} - {level} - {message}\n"

        # Use self.log_textbox now
        self.log_textbox.configure(state=tk.NORMAL)
        self.log_textbox.insert(tk.END, log_entry)
        self.log_textbox.configure(state=tk.DISABLED)
        self.log_textbox.see(tk.END)
        self.update_idletasks()

    def set_ui_state(self, running: bool):
        """Enable/disable UI elements based on running state."""
        self.running_state = running
        state = tk.DISABLED if running else tk.NORMAL
        button_state = tk.NORMAL if running else tk.DISABLED

        # Disable input fields when running (no changes here)
        for entry in [self.pg_host_entry, self.pg_port_entry, self.pg_user_entry,
                      self.pg_pass_entry, self.pg_dbname_entry,
                      self.mysql_host_entry, self.mysql_port_entry,
                      self.mysql_dbz_user_entry, self.mysql_dbz_pass_entry,
                      self.mysql_admin_user_entry, self.mysql_admin_pass_entry]:
            entry.configure(state=state)

        self.start_button.configure(state=state)
        self.stop_button.configure(state=button_state)
    
    def toggle_pg_fields(self):
        """Enable or disable PostgreSQL connection fields based on selection."""
        state = tk.NORMAL if self.enable_pg_var.get() else tk.DISABLED
        for widget in [self.pg_host_entry, self.pg_port_entry,
                       self.pg_user_entry, self.pg_pass_entry,
                       self.pg_dbname_entry]:
            widget.configure(state=state)

    def toggle_mysql_fields(self):
        """Enable or disable MySQL connection fields based on selection."""
        state = tk.NORMAL if self.enable_mysql_var.get() else tk.DISABLED
        for widget in [self.mysql_host_entry, self.mysql_port_entry,
                       self.mysql_dbz_user_entry, self.mysql_dbz_pass_entry,
                       self.mysql_admin_user_entry, self.mysql_admin_pass_entry]:
            widget.configure(state=state)

    def clear_output_tabs(self):
        """Clears the content of both the Logs and Consumer Output textboxes."""
        logger.info("Clearing output tabs...")
        # Clear Log Textbox
        try:
            self.log_textbox.configure(state=tk.NORMAL)
            self.log_textbox.delete("1.0", tk.END)
            self.log_textbox.configure(state=tk.DISABLED)
        except Exception as e:
             logger.error(f"Failed to clear log textbox: {e}")

        # Clear Consumer Output Textbox
        try:
            self.consumer_output_textbox.configure(state=tk.NORMAL)
            self.consumer_output_textbox.delete("1.0", tk.END)
            self.consumer_output_textbox.configure(state=tk.DISABLED)
        except Exception as e:
             logger.error(f"Failed to clear consumer output textbox: {e}")
        logger.info("Output tabs cleared.")


    # --- start_tracking_thread (Call clear before starting) ---
    def start_tracking_thread(self):
        self.clear_output_tabs() # Clear previous output before starting new run
        """Starts the tracking process in a separate thread to keep GUI responsive."""
        self.set_ui_state(running=True)

        # Clear Log Textbox
        self.log_textbox.configure(state=tk.NORMAL)
        self.log_textbox.delete("1.0", tk.END)
        self.log_textbox.configure(state=tk.DISABLED)

        # Clear Consumer Output Textbox
        self.consumer_output_textbox.configure(state=tk.NORMAL)
        self.consumer_output_textbox.delete("1.0", tk.END)
        self.consumer_output_textbox.configure(state=tk.DISABLED)

        self.add_status_message("Starting tracking process...")

        # Start the main logic in a new thread
        thread = threading.Thread(target=self.run_tracking_logic, daemon=True)
        thread.start()

    def run_tracking_logic(self):
        """The core logic executed in the background thread."""
        try:
            # --- Steps 1-5 (Get Credentials, Docker Setup, Wait, DB Init, Connector Setup) ---
            # ... (These sections remain largely the same as before) ...
            # ... (Ensure logging messages go to the main logger) ...
            pg_details = {
                "host": self.pg_host_entry.get() or "localhost",
                "port": int(self.pg_port_entry.get() or config.DEFAULT_POSTGRES_PORT),
                "user": self.pg_user_entry.get(),
                "password": self.pg_pass_entry.get(),
                "dbname": self.pg_dbname_entry.get(),
            }
            mysql_details = {
                "host": self.mysql_host_entry.get() or "localhost",
                "port": int(self.mysql_port_entry.get() or config.DEFAULT_MYSQL_PORT),
                "dbz_user": self.mysql_dbz_user_entry.get(),
                "dbz_password": self.mysql_dbz_pass_entry.get(),
                "admin_user": self.mysql_admin_user_entry.get(),
                "admin_password": self.mysql_admin_pass_entry.get(),
            }

            # Validate enabled connections and required details
            enabled_pg = self.enable_pg_var.get()
            enabled_mysql = self.enable_mysql_var.get()
            if not (enabled_pg or enabled_mysql):
                raise ValueError("Select at least one database connection to track.")
            if enabled_pg and not all([pg_details['user'], pg_details['password'], pg_details['dbname']]):
                raise ValueError("Missing required PostgreSQL connection details.")
            if enabled_mysql and not all([mysql_details['dbz_user'], mysql_details['dbz_password'], mysql_details['admin_user']]):
                raise ValueError("Missing required MySQL connection details.")

            if not docker_manager.start_services():
                raise RuntimeError("Failed to start Docker services...") # Shortened for brevity
            if not connector_manager.wait_for_connect_ready():
                 raise RuntimeError("Kafka Connect service did not become ready...")

            init_ok = True
            # Initialize PostgreSQL if enabled
            if enabled_pg:
                logger.info("--- Initializing PostgreSQL ---")
                if not db_initializer.initialize_postgres(
                    host=pg_details['host'], port=pg_details['port'],
                    user=pg_details['user'], password=pg_details['password'],
                    dbname=pg_details['dbname']
                ):
                    logger.warning("PostgreSQL initialization completed with errors/warnings...")
            else:
                logger.info("Skipping PostgreSQL initialization per selection...")

            # Initialize MySQL if enabled
            if enabled_mysql:
                logger.info("--- Initializing MySQL ---")
                if not db_initializer.initialize_mysql(
                    db_host=mysql_details['host'], db_port=mysql_details['port'],
                    db_user=mysql_details['admin_user'], db_password=mysql_details['admin_password'],
                    debezium_user=mysql_details['dbz_user'], debezium_password=mysql_details['dbz_password']
                ):
                    logger.error("MySQL initialization failed...")
                    init_ok = False
            else:
                logger.info("Skipping MySQL initialization per selection...")

            if not init_ok: raise RuntimeError("Database initialization failed...")

            logger.info("--- Configuring Debezium Connectors ---")
            # Delete existing connectors based on selection
            if enabled_pg:
                connector_manager.delete_connector(config.POSTGRES_CONNECTOR_NAME)
            if enabled_mysql:
                connector_manager.delete_connector(config.MYSQL_CONNECTOR_NAME)
            time.sleep(2)

            pg_conn_ok = False
            if enabled_pg:
                pg_conn_ok = connector_manager.create_postgres_connector(
                    host=pg_details['host'], port=pg_details['port'],
                    user=pg_details['user'], password=pg_details['password'],
                    dbname=pg_details['dbname']
                )
                if pg_conn_ok:
                    logger.info("PostgreSQL connector configured.")
                else:
                    logger.error("Failed to create/update PostgreSQL connector...")
            else:
                logger.info("Skipping PostgreSQL connector creation per selection.")

            mysql_conn_ok = False
            if enabled_mysql:
                # Generate unique server identifiers
                mysql_server_id = str(abs(hash(f"{mysql_details['host']}:{mysql_details['port']}")) % (2**31))
                mysql_server_name = f"mysql_{mysql_details['host'].replace('.', '_')}_{mysql_details['port']}"
                mysql_conn_ok = connector_manager.create_mysql_connector(
                    db_host=mysql_details['host'], db_port=mysql_details['port'],
                    db_user=mysql_details['dbz_user'], db_password=mysql_details['dbz_password'],
                    db_server_id=mysql_server_id, db_server_name=mysql_server_name
                )
                if mysql_conn_ok:
                    logger.info("MySQL connector configured.")
                else:
                    logger.error("Failed to create/update MySQL connector...")
            else:
                logger.info("Skipping MySQL connector creation per selection.")

            # Verify connector setup for enabled databases
            if enabled_pg and not pg_conn_ok:
                raise RuntimeError("Failed to configure PostgreSQL connector.")
            if enabled_mysql and not mysql_conn_ok:
                raise RuntimeError("Failed to configure MySQL connector.")

            # --- Start Consumer ---
            logger.info("--- Starting Kafka Consumer ---")
            self.stop_consumer_event.clear()
            # Pass the queueing callback method to the consumer thread
            self.consumer_thread = threading.Thread(
                target=consumer.run_consumer,
                args=(self.stop_consumer_event, self.queue_consumer_message), # Pass queue method
                daemon=True)
            self.consumer_thread.start()
            logger.info("Change consumer thread started. Monitoring changes in 'Consumer Output' tab.")
            self.add_status_message("Tracking active. See 'Consumer Output' tab for change events.", level="SUCCESS")

        except ValueError as e:
             logger.error(f"Configuration Error: {e}")
             self.add_status_message(f"Configuration Error: {e}", level="ERROR")
             self.set_ui_state(running=False)
        except RuntimeError as e:
             logger.error(f"Runtime Error: {e}")
             self.add_status_message(f"Failed to start: {e}", level="ERROR")
             self.stop_tracking() # Attempt cleanup on runtime error
        except Exception as e:
            logger.critical(f"Unexpected error during startup: {e}", exc_info=True)
            self.add_status_message(f"Unexpected critical error: {e}", level="CRITICAL")
            self.stop_tracking()


    def stop_tracking(self):
        """Initiates stopping of the consumer thread and Docker containers in a background thread."""
        # Prevent duplicate stop attempts
        if not self.running_state and self.stop_button.cget('state') == tk.DISABLED:
            logger.debug("Stop tracking already initiated or not running.")
            return

        # Update UI immediately to show stopping state
        self.add_status_message("Stopping tracking process...")
        logger.info("Stopping tracking process...")
        self.set_ui_state(running=True)
        self.stop_button.configure(text="Stopping...", state=tk.DISABLED)

        # Perform stop logic in background to keep UI responsive
        threading.Thread(target=self._stop_tracking_async, daemon=True).start()

    def _stop_tracking_async(self):
        """Background thread logic to stop consumer thread and Docker services."""
        try:
            # 1. Signal the consumer thread to stop
            if self.consumer_thread and self.consumer_thread.is_alive():
                logger.info("Signaling consumer thread to stop...")
                self.stop_consumer_event.set()
                # Wait for the thread to finish
                self.consumer_thread.join(timeout=10)
                if self.consumer_thread.is_alive():
                    logger.warning("Consumer thread did not stop gracefully within timeout.")
                else:
                    logger.info("Consumer thread stopped.")
            self.consumer_thread = None
            self.stop_consumer_event.clear()

            # 2. Stop Docker containers
            logger.info("Stopping Docker services...")
            docker_manager.stop_services()
            logger.info("Docker services stopped.")
        except Exception as e:
            logger.error(f"Error during stop tracking: {e}", exc_info=True)
        finally:
            # Schedule UI update back on the main thread
            self.after(0, self._complete_stop_tracking_ui)

    def _complete_stop_tracking_ui(self):
        """Finalize UI state after stopping tracking."""
        self.add_status_message("Tracking stopped.", level="INFO")
        self.set_ui_state(running=False)
        self.stop_button.configure(text="Stop Tracking")


    def on_closing(self):
        """Handle window close event."""
        logger.info("Close window requested.")
        if self.running_state:
            self.add_status_message("Window closed while tracking active. Initiating stop...")
            self.stop_tracking()
            # Need to ensure stop_tracking finishes before destroying
            # Maybe disable close button while stopping? Or wait briefly.
            # For now, assume stop_tracking is reasonably fast.
        # Clean up logger handler
        root_logger = logging.getLogger()
        root_logger.removeHandler(self.log_handler)
        # Clean up queue polling
        # This should ideally cancel the `after` call, but destroying window might suffice
        logger.info("Destroying GUI.")
        self.destroy()

# --- Logging Handler for GUI Textbox ---
class GUILoggingHandler(logging.Handler):
    def __init__(self, textbox):
        super().__init__()
        self.textbox = textbox
        self.queue = queue.Queue()
        # Start polling the queue for log messages
        self.textbox.after(100, self._process_queue)


    def emit(self, record):
        """Send log message to the queue"""
        msg = self.format(record)
        self.queue.put(msg)

    def _process_queue(self):
         """Process log messages from queue in GUI thread"""
         try:
             while True:
                 msg = self.queue.get_nowait()
                 self._append_message(msg)
         except queue.Empty:
             pass # No messages left
         finally:
             # Reschedule polling
             self.textbox.after(100, self._process_queue)

    def _append_message(self, msg):
         """Append message to the textbox"""
         try:
             self.textbox.configure(state=tk.NORMAL)
             self.textbox.insert(tk.END, msg + "\n")
             self.textbox.configure(state=tk.DISABLED)
             self.textbox.see(tk.END)
         except Exception as e:
              print(f"Error updating GUI logger: {e}") # Print to console if GUI logging fails