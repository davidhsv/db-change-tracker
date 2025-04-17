# Database Change Tracker (Standalone Python App)

A real-time monitoring tool for database changes using Debezium, Kafka (managed via Docker), and a Python GUI.

## Overview

This standalone Python application captures and displays Change Data Capture (CDC) events from user-specified MySQL and PostgreSQL databases in real-time. It provides a simple GUI to enter database credentials, then transparently starts the necessary Kafka/Debezium infrastructure using Docker. The application helps developers and testers observe database changes (INSERT, UPDATE, DELETE) directly in the console.

## Features

- **Standalone Application:** Python-based tool with a GUI.
- **Transparent Docker Management:** Automatically starts/stops required Zookeeper, Kafka, and Kafka Connect containers.
- **GUI Configuration:** Easily input connection details for PostgreSQL and MySQL.
- **Database Initialization:** Attempts to run required setup SQL (MySQL `GRANT`, PostgreSQL `REPLICA IDENTITY FULL`) on target databases.
- **Real-Time Monitoring:** Displays color-coded change events in the console.
- **Multi-Database Support:** Can monitor both PostgreSQL and MySQL simultaneously (requires separate connection details).

## Prerequisites

- **Python:** Version 3.9 or higher.
- **uv:** The Python package installer and virtual environment manager (`pip install uv`).
- **Docker:** Docker Desktop or Docker Engine must be installed and running. The application needs permissions to interact with the Docker daemon.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repo-url>
    cd db-change-tracker
    ```

2. **Install dependencies:**
    ```bash
    brew install python-tk
    ```

3.  **Run the application:**
    ```bash
    uv run db-change-tracker
    ```
    
4.  **Enter Database Details:**
    *   Fill in the host, port, username, password, and database name for the PostgreSQL instance you want to monitor. The user provided should have permissions to connect and ideally permissions to run `ALTER TABLE` (for `REPLICA IDENTITY`).
    *   Fill in the host, port, and **Debezium user credentials** for the MySQL instance. This is the user Debezium will use to connect.
    *   Fill in the **Admin user credentials** for MySQL (e.g., `root`). This user needs privileges to grant permissions to the Debezium user. *The admin password is only used once during initialization.*
    *   Leave sections blank if you only want to monitor one database type.

5.  **Start Tracking:**
    *   Click the "Start Tracking" button.
    *   The application will:
        *   Start the required Docker containers (Zookeeper, Kafka, Kafka Connect). This might take a minute on the first run as images are pulled.
        *   Attempt to connect to your databases using the provided credentials to run initialization SQL (GRANTs for MySQL, REPLICA IDENTITY for PostgreSQL). Check the status window in the GUI for success or failure messages.
        *   Configure the Debezium connectors via the Kafka Connect API.
        *   Start the Kafka consumer thread.
    *   Status messages will appear in the GUI's text box.
    *   **Database change events will appear in the console/terminal where you launched the application.**

6.  **Stop Tracking:**
    *   Click the "Stop Tracking" button in the GUI.
    *   This will stop the consumer thread and shut down/remove the Docker containers.

## Important Notes & Troubleshooting

*   **Docker:** Ensure Docker is running *before* starting the application.
*   **Database Permissions:**
    *   **PostgreSQL:** The user provided for PostgreSQL needs `CONNECT` privileges on the database. For `REPLICA IDENTITY FULL` to be set automatically, the user ideally needs `ALTER TABLE` rights on the tables to be monitored, or you need to run the `ALTER TABLE ... REPLICA IDENTITY FULL;` command manually for each required table using a superuser *before* starting the tracker. The application attempts this but might fail due to permissions. Debezium *requires* `REPLICA IDENTITY FULL` (or `USING INDEX`) for `UPDATE` and `DELETE` events on non-keyed tables or when `before` data is needed. The user also needs permissions to create replication slots (`CREATE SLOT`) and publications (`CREATE PUBLICATION`) if the connector configuration requires them (which the default does). Granting `REPLICATION` or `rds_replication` role might be necessary.
    *   **MySQL:** The **Admin User** needs privileges to `GRANT` permissions (typically `root` or a user with `GRANT OPTION`). The **Debezium User** needs `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, `REPLICATION CLIENT`. The application attempts to grant these using the admin credentials. Binary logging must be enabled on your MySQL server (`log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`).
*   **Firewall:** Ensure your firewall allows connections from the application to your database hosts/ports, and locally to ports `9092` (Kafka) and `8083` (Kafka Connect) used by the Docker containers.
*   **Console Output:** Database change events are printed to the standard output (the terminal where you ran `db-change-tracker`), not the GUI status window. The GUI window shows setup progress and errors.
*   **Error Logs:** Check both the GUI status window and the console output for detailed error messages if startup fails. Docker container logs can also be checked manually if needed (`docker logs kafka-connect-dbtracker`, `docker logs kafka-dbtracker`).
*   **Resource Usage:** Running Kafka and related services uses system resources (RAM, CPU).

## Color Legend (Console Output)

- **Green (+):** New/inserted data or the 'after' state in updates
- **Red (-):** Removed data or the 'before' state in updates
- **Yellow:** Update operation headers / Timestamps
- **Cyan:** Operation and table information headers
- **Blue:** Field names
- **Light Black/Gray:** Unchanged fields during updates
- **Magenta:** Section headers (INSERTED/DELETED/UPDATED)