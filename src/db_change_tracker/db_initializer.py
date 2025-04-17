import logging
import psycopg2
from psycopg2 import sql
import mysql.connector
from mysql.connector import errorcode

logger = logging.getLogger(__name__)

def initialize_postgres(host, port, user, password, dbname) -> bool:
    """
    Connects to PostgreSQL and attempts to set REPLICA IDENTITY FULL
    for all user tables in the specified database.
    """
    logger.info(f"Initializing PostgreSQL settings for database '{dbname}' on {host}:{port}...")
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            connect_timeout=10
        )
        conn.autocommit = True # Execute each statement immediately
        cursor = conn.cursor()
        logger.info("Successfully connected to PostgreSQL.")

        # Query to find tables needing REPLICA IDENTITY FULL
        # Excludes system schemas, partitioned tables ('p'), foreign tables ('f')
        # Targets ordinary tables ('r') that are permanent ('p')
        find_tables_sql = """
        SELECT n.nspname, c.relname
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND c.relpersistence = 'p'
          AND n.nspname NOT LIKE 'pg_%'
          AND n.nspname != 'information_schema'
          AND c.relreplident != 'f';
        """
        cursor.execute(find_tables_sql)
        tables_to_alter = cursor.fetchall()

        if not tables_to_alter:
            logger.info("No user tables found or all tables already have REPLICA IDENTITY FULL.")
            return True

        logger.info(f"Found {len(tables_to_alter)} user table(s) to check/set REPLICA IDENTITY FULL.")

        success_count = 0
        failure_count = 0
        for schema, table in tables_to_alter:
            try:
                alter_sql = sql.SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(
                    sql.Identifier(schema), sql.Identifier(table)
                )
                logger.debug(f"Executing: {alter_sql.as_string(cursor)}")
                cursor.execute(alter_sql)
                logger.info(f"Set REPLICA IDENTITY FULL for table '{schema}.{table}'.")
                success_count += 1
            except psycopg2.Error as e:
                # Common errors: permission denied (55000), object not found (42P01)
                # We log but continue, as the user might not own all tables
                logger.warning(f"Could not set REPLICA IDENTITY FULL for '{schema}.{table}': {e.pgcode} - {e.pgerror}")
                failure_count += 1
                # Optional: Re-raise if it's a critical error?
                # For now, we allow it to proceed

        logger.info(f"PostgreSQL initialization finished. Success: {success_count}, Failures/Skipped: {failure_count}")
        cursor.close()
        return failure_count == 0 # Return True only if all attempts succeeded

    except psycopg2.OperationalError as e:
        logger.error(f"PostgreSQL connection failed for '{dbname}' on {host}:{port}: {e}")
        return False
    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error during initialization: {e.pgcode} - {e.pgerror}")
        return False
    except Exception as e:
         logger.error(f"Unexpected error during PostgreSQL initialization: {e}", exc_info=True)
         return False
    finally:
        if conn:
            conn.close()
            logger.debug("PostgreSQL connection closed.")


def initialize_mysql(db_host, db_port, db_user, db_password, debezium_user, debezium_password) -> bool:
    """
    Connects to MySQL (as root/admin user) and grants necessary privileges
    to the Debezium user.
    """
    logger.info(f"Initializing MySQL grants for user '{debezium_user}' on {db_host}:{db_port}...")
    conn = None
    try:
        # Connect using the provided admin credentials (e.g., root)
        conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            connect_timeout=10
        )
        cursor = conn.cursor()
        logger.info("Successfully connected to MySQL as admin user.")

        # Grant privileges required by Debezium
        grant_sql = f"""
        GRANT RELOAD, REPLICATION SLAVE, REPLICATION CLIENT, SELECT, RELOAD, LOCK TABLES
        ON *.*
        TO '{debezium_user}'@'%';
        """

        logger.info(f"Granting privileges to MySQL user '{debezium_user}'...")
        logger.debug(f"Executing GRANT statement...")
        cursor.execute(grant_sql) # Execute grants separately

        flush_sql = "FLUSH PRIVILEGES;"
        logger.debug(f"Executing: {flush_sql}")
        cursor.execute(flush_sql)

        logger.info(f"Successfully granted privileges to MySQL user '{debezium_user}'.")
        cursor.close()
        return True

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error(f"MySQL Access Denied: Check admin user ('{db_user}') credentials for {db_host}:{db_port}.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error(f"MySQL Database does not exist (if a specific DB was used in connect).")
        elif err.errno == errorcode.CR_CONN_HOST_ERROR:
             logger.error(f"MySQL connection to host '{db_host}:{db_port}' failed. Check host/port and network access.")
        else:
            logger.error(f"MySQL error during initialization: {err}")
        return False
    except Exception as e:
         logger.error(f"Unexpected error during MySQL initialization: {e}", exc_info=True)
         return False
    finally:
        if conn and conn.is_connected():
            conn.close()
            logger.debug("MySQL connection closed.")