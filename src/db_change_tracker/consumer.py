import json
import time
import logging
import base64
from datetime import datetime, date, timedelta
from decimal import Decimal, Context as DecimalContext
from kafka import KafkaConsumer, errors as KafkaErrors
# Colorama is no longer needed for direct printing if callback handles colors
# from colorama import Fore, Style, init
import threading
import traceback # For detailed error logging

from . import config

# init(autoreset=True) # Not needed if not printing directly with colorama
logger = logging.getLogger(__name__)

# --- Style Tag Identifiers (must match keys in TAG_COLORS in gui.py) ---
STYLE_HEADER = "header"
STYLE_OP_INSERT = "op_insert"
STYLE_OP_UPDATE = "op_update"
STYLE_OP_DELETE = "op_delete"
STYLE_OP_READ = "op_read"
STYLE_OP_UNKNOWN = "op_unknown"
STYLE_SECTION = "section"
STYLE_FIELD_NAME = "field_name"
STYLE_BEFORE = "before"
STYLE_AFTER = "after"
STYLE_UNCHANGED = "unchanged"
STYLE_TIMESTAMP = "timestamp"
STYLE_SEPARATOR = "separator"
STYLE_ERROR = "error"
STYLE_NORMAL = "normal"

# --- Logical Type Names (remain the same) ---
# ... (LOGICAL_TYPE_DATE, etc.) ...
LOGICAL_TYPE_DATE = "io.debezium.time.Date"
LOGICAL_TYPE_TIMESTAMP_MS = "io.debezium.time.Timestamp"
LOGICAL_TYPE_TIMESTAMP_US = "io.debezium.time.MicroTimestamp"
LOGICAL_TYPE_TIMESTAMP_NS = "io.debezium.time.NanoTimestamp"
LOGICAL_TYPE_DEBEZIUM_DECIMAL = "io.debezium.data.VariableScaleDecimal" # Debezium specific
LOGICAL_TYPE_CONNECT_DECIMAL = "org.apache.kafka.connect.data.Decimal" # Kafka Connect standard


# --- Helper Functions (remain the same) ---
# ... (format_timestamp_ms, format_timestamp_us, etc.) ...
def format_timestamp_ms(ts_ms):
    if ts_ms is None: return "null"
    try:
        ts_ms = int(ts_ms)
        if ts_ms < -62135596800000: return f"<{ts_ms} ms (Pre-Epoch)>"
        # Use UTC to avoid local timezone conversion
        dt = datetime.utcfromtimestamp(ts_ms / 1000.0)
        # Format to milliseconds precision and append UTC indicator
        ts_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"
        return ts_str
    except (ValueError, TypeError, OverflowError) as e:
        logger.debug(f"Timestamp MS format error for {ts_ms}: {e}")
        return str(ts_ms)
def format_timestamp_us(ts_us):
    if ts_us is None: return "null"
    try:
        ts_us = int(ts_us)
        if ts_us < -62135596800000000: return f"<{ts_us} µs (Pre-Epoch)>"
        seconds = ts_us // 1_000_000
        microseconds = ts_us % 1_000_000
        # Use UTC to avoid local timezone conversion
        dt = datetime.utcfromtimestamp(seconds)
        dt_with_micros = dt.replace(microsecond=microseconds)
        # Append UTC indicator
        return dt_with_micros.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC"
    except (ValueError, TypeError, OverflowError) as e:
        logger.debug(f"Timestamp US format error for {ts_us}: {e}")
        return str(ts_us)
def format_timestamp_ns(ts_ns):
    if ts_ns is None: return "null"
    try:
        ts_ns = int(ts_ns)
        if ts_ns < -62135596800000000000: return f"<{ts_ns} ns (Pre-Epoch)>"
        seconds = ts_ns // 1_000_000_000
        nanos_remainder = ts_ns % 1_000_000_000
        microseconds = nanos_remainder // 1000
        # Use UTC to avoid local timezone conversion
        dt = datetime.utcfromtimestamp(seconds)
        dt_with_micros = dt.replace(microsecond=microseconds)
        extra_info = f" (ns truncated: {nanos_remainder % 1000})" if nanos_remainder % 1000 else ""
        # Append UTC indicator before any extra info
        return dt_with_micros.strftime(f"%Y-%m-%d %H:%M:%S.%f") + " UTC" + extra_info
    except (ValueError, TypeError, OverflowError) as e:
        logger.debug(f"Timestamp NS format error for {ts_ns}: {e}")
        return str(ts_ns)
def decode_debezium_date(value):
    if value is None: return "null"
    if isinstance(value, int):
        try:
            epoch = date(1970, 1, 1)
            d = epoch + timedelta(days=value)
            return d.isoformat()
        except (OverflowError, TypeError, ValueError) as e:
             logger.debug(f"Date decode error for {value}: {e}")
             return f"{value} [Date Decode Error]"
    return json.dumps(value)
def decode_debezium_variable_scale_decimal(value):
    if value is None: return "null"
    if isinstance(value, dict) and 'scale' in value and 'value' in value:
        try:
            scale = int(value['scale'])
            val_b64 = value['value']
            if isinstance(val_b64, str): val_bytes = base64.b64decode(val_b64)
            elif isinstance(val_b64, bytes): val_bytes = val_b64
            else: raise TypeError("Decimal 'value' field is not bytes or base64 string")
            unscaled = int.from_bytes(val_bytes, byteorder='big', signed=True)
            if scale == 0: return str(unscaled)
            # Use higher precision context if needed
            dec_val = Decimal(unscaled).scaleb(-scale, context=DecimalContext(prec=50))
            return str(dec_val)
        except (TypeError, ValueError, base64.binascii.Error, OverflowError) as e:
            logger.debug(f"VariableScaleDecimal decode error for {value}: {e}")
            return json.dumps(value)
    return json.dumps(value)
def decode_connect_decimal(value, parameters):
    if value is None: return "null"
    if isinstance(value, str) and parameters and isinstance(parameters, dict):
        try:
            scale_str = parameters.get('scale')
            if scale_str is None: raise ValueError("Scale parameter missing")
            scale = int(scale_str)
            val_bytes = base64.b64decode(value)
            unscaled = int.from_bytes(val_bytes, byteorder='big', signed=True)
            if scale == 0: return str(unscaled)
            dec_val = Decimal(unscaled).scaleb(-scale, context=DecimalContext(prec=50))
            return str(dec_val)
        except (TypeError, ValueError, base64.binascii.Error, OverflowError) as e:
            logger.debug(f"Connect Decimal decode error for {value} with params {parameters}: {e}")
            return f"{value} [Decimal Decode Error]"
    return json.dumps(value)
def extract_field_types_from_schema(message_schema):
    # No changes needed here
    field_types = {}
    if not message_schema or not isinstance(message_schema, dict): return field_types
    payload_schema_def = None
    top_level_fields = message_schema.get('fields', [])
    schema_for_payload = message_schema
    if isinstance(top_level_fields, list) and len(top_level_fields) > 0:
        is_envelope = any(f.get('field') in ('before', 'after', 'source', 'op', 'ts_ms') for f in top_level_fields if isinstance(f, dict))
        if is_envelope:
            for field_def in top_level_fields:
                if isinstance(field_def, dict) and field_def.get('field') in ('after', 'before'):
                     if field_def.get('type') == 'struct' and isinstance(field_def.get('fields'), list):
                          schema_for_payload = field_def
                          if field_def.get('field') == 'after': break
    payload_fields = schema_for_payload.get('fields', [])
    if not isinstance(payload_fields, list): return field_types
    for field_detail in payload_fields:
        if isinstance(field_detail, dict):
            field_name = field_detail.get('field')
            if not field_name: continue
            logical_type = field_detail.get('name')
            parameters = field_detail.get('parameters')
            base_type = field_detail.get('type', 'unknown')
            type_info = {
                "logical_type": logical_type,
                "parameters": parameters if isinstance(parameters, dict) else {},
                "base_type": base_type
            }
            field_types[field_name] = type_info
    return field_types
def decode_value_based_on_type(value, type_info):
    # No changes needed here
    if value is None: return "null"
    if not type_info or not isinstance(type_info, dict):
        if isinstance(value, bytes):
             try: return value.decode('utf-8')
             except UnicodeDecodeError: return f"b64:{base64.b64encode(value).decode('ascii')}"
        else: return json.dumps(value, ensure_ascii=False)
    logical_type = type_info.get("logical_type")
    parameters = type_info.get("parameters", {})
    base_type = type_info.get("base_type")
    if logical_type == LOGICAL_TYPE_DATE: return decode_debezium_date(value)
    elif logical_type == LOGICAL_TYPE_DEBEZIUM_DECIMAL: return decode_debezium_variable_scale_decimal(value)
    elif logical_type == LOGICAL_TYPE_CONNECT_DECIMAL: return decode_connect_decimal(value, parameters)
    elif logical_type == LOGICAL_TYPE_TIMESTAMP_MS: return format_timestamp_ms(value)
    elif logical_type == LOGICAL_TYPE_TIMESTAMP_US: return format_timestamp_us(value)
    elif logical_type == LOGICAL_TYPE_TIMESTAMP_NS: return format_timestamp_ns(value)
    if base_type == 'bytes':
         if isinstance(value, str): return f"b64:{value}"
         elif isinstance(value, bytes):
              try: return value.decode('utf-8')
              except UnicodeDecodeError:
                  try: return value.decode('latin-1')
                  except UnicodeDecodeError: return f"b64:{base64.b64encode(value).decode('ascii')}"
         else: return json.dumps(value, ensure_ascii=False)
    try: return json.dumps(value, ensure_ascii=False)
    except TypeError: return str(value)


# --- Send Output function (replaces print for styled output) ---
def send_output(callback, text, style_tag=STYLE_NORMAL):
    """Sends text segment and style tag to the callback if available."""
    if callback:
        callback(text, style_tag)
    # else:
        # Optional: print to console if no callback (for standalone testing)
        # print(text, end="") # Suppress newline if sending segments

# --- print_colored_diff (Modified to use send_output) ---
def print_colored_diff(before_data, after_data, field_types, callback):
    """Sends colored diff segments to the callback."""
    is_insert = (before_data is None and after_data is not None)
    is_delete = (before_data is not None and after_data is None)
    is_update = (before_data is not None and after_data is not None)

    if not (is_insert or is_delete or is_update):
        send_output(callback, f"  (No before/after data present or identical)\n", STYLE_UNCHANGED)
        return

    data_to_iterate = after_data if after_data is not None else before_data
    if data_to_iterate is None:
         send_output(callback, f"  (Error: Relevant data dictionary is missing)\n", STYLE_ERROR)
         logger.warning("Diff function called with missing before/after data.")
         return

    try:
         sorted_keys = sorted(data_to_iterate.keys(), key=str)
    except Exception as e:
         logger.warning(f"Could not sort field keys: {e}. Using unsorted keys.")
         sorted_keys = list(data_to_iterate.keys())

    # --- Send Header ---
    if is_insert:
        send_output(callback, "INSERTED DATA:\n", STYLE_SECTION)
    elif is_delete:
        send_output(callback, "DELETED DATA:\n", STYLE_SECTION)
    elif is_update:
        has_changes = any(before_data.get(key) != after_data.get(key) for key in sorted_keys if key in before_data)
        if has_changes:
            send_output(callback, "UPDATED FIELDS:\n", STYLE_SECTION)
        else:
            send_output(callback, "UPDATE (No field changes detected):\n", STYLE_SECTION)

    # --- Iterate and Send Fields ---
    for key in sorted_keys:
        before_value = before_data.get(key) if before_data else None
        after_value = after_data.get(key) if after_data else None
        type_info = field_types.get(key)

        try:
            before_decoded = decode_value_based_on_type(before_value, type_info)
            after_decoded = decode_value_based_on_type(after_value, type_info)
        except Exception as e:
             logger.error(f"Error decoding field '{key}' value: {e}", exc_info=True)
             before_decoded = f"[Decode Error: {before_value}]"
             after_decoded = f"[Decode Error: {after_value}]"
             send_output(callback, f"  Error decoding field {key}\n", STYLE_ERROR) # Indicate decode error

        if is_insert:
            send_output(callback, f"  {key}: ", STYLE_FIELD_NAME)
            send_output(callback, f"{after_decoded}\n", STYLE_AFTER)
        elif is_delete:
            send_output(callback, f"  {key}: ", STYLE_FIELD_NAME)
            send_output(callback, f"{before_decoded}\n", STYLE_BEFORE)
        elif is_update:
            if before_value != after_value: # Changed field
                send_output(callback, f"  {key}:\n", STYLE_FIELD_NAME)
                send_output(callback, f"    - {before_decoded}\n", STYLE_BEFORE)
                send_output(callback, f"    + {after_decoded}\n", STYLE_AFTER)
            else: # Unchanged field
                send_output(callback, f"  {key}: ", STYLE_UNCHANGED)
                send_output(callback, f"{after_decoded}\n", STYLE_UNCHANGED)


# --- Main Consumer Logic (Modified) ---
def run_consumer(stop_event: threading.Event, gui_callback: callable = None):
    """Connects to Kafka and processes messages until stop_event is set."""
    consumer = None
    listening_message_sent = False
    
    while not stop_event.is_set():
        try:
            # --- Consumer Creation / Subscription (Mostly unchanged) ---
            if consumer is None:
                logger.info(f"Attempting to connect Kafka consumer to {config.CONSUMER_BOOTSTRAP_SERVERS}...")
            consumer = KafkaConsumer(
                bootstrap_servers=config.CONSUMER_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8', errors='replace')) if x else None, # Use errors='replace' for robustness
                key_deserializer=lambda x: x.decode('utf-8', errors='replace') if x else None,
                group_id=config.CONSUMER_GROUP_ID,
                consumer_timeout_ms=1000,
                enable_auto_commit=True,
                security_protocol='PLAINTEXT',
            )
            # Subscribe to topics before any reset or UI messages
            consumer.subscribe(pattern=config.CONSUMER_TOPIC_PATTERN)
            # On first creation, advance offsets to end so only new messages are consumed
            if not listening_message_sent:
                try:
                    consumer.poll(timeout_ms=1000)  # trigger partition assignment
                    parts = consumer.assignment()
                    if parts:
                        consumer.seek_to_end(*parts)
                        logger.info(f"Consumer offsets moved to end for partitions: {parts}")
                except Exception as e:
                    logger.warning(f"Could not advance consumer offsets to end: {e}")
                # Initial UI notifications
                logger.info(f"Kafka Consumer created. Subscribing to pattern: {config.CONSUMER_TOPIC_PATTERN}")
                send_output(gui_callback, "--- CONSUMER STARTED ---\n", STYLE_HEADER)
                send_output(gui_callback, f"Subscribed to topics matching: {config.CONSUMER_TOPIC_PATTERN}\n", STYLE_NORMAL)
                send_output(gui_callback, "Listening for database changes...\n", STYLE_NORMAL)
                listening_message_sent = True
    
            # --- Main Polling Loop ---
            while not stop_event.is_set():
                try:
                    # Refresh subscription to pick up new topics matching pattern
                    consumer.subscribe(pattern=config.CONSUMER_TOPIC_PATTERN)
                    message_batch = consumer.poll(timeout_ms=1000)
                    if not message_batch: continue
    
                    logger.debug(f"Polled {sum(len(msgs) for msgs in message_batch.values())} messages.") # DEBUG: See if polling gets anything
    
                    for tp, messages in message_batch.items():
                        if stop_event.is_set(): break
                        logger.debug(f"Processing batch for TopicPartition: {tp}") # DEBUG: See which topics have messages
    
                        for message in messages:
                            # --- START OF INDIVIDUAL MESSAGE PROCESSING ---
                            if stop_event.is_set(): break
                            logger.info(f"Processing message: Offset={message.offset}, Topic='{message.topic}', Key='{message.key}'") # INFO: Log every message start
    
                            if not message.value or not isinstance(message.value, dict):
                                logger.warning(f"Skipping message Offset={message.offset} due to invalid value: {message.value}")
                                continue
    
                            # DEBUG: Log the raw value structure (first level keys)
                            logger.debug(f"Message Offset={message.offset} Value Keys: {list(message.value.keys())}")
    
                            message_schema = message.value.get('schema')
                            payload = message.value.get('payload')
    
                            if not payload or not isinstance(payload, dict):
                                logger.warning(f"Skipping message Offset={message.offset} due to missing/invalid payload. Value keys: {list(message.value.keys())}")
                                continue
    
                            # DEBUG: Log payload structure (first level keys)
                            logger.debug(f"Message Offset={message.offset} Payload Keys: {list(payload.keys())}")
    
                            op_type = payload.get('op', '?')
                            before_data = payload.get('before')
                            after_data = payload.get('after')
    
                            if op_type == 'u' and before_data == after_data:
                                logger.debug(f"Skipping no-op update message Offset={message.offset}")
                                continue
    
                            source = payload.get('source', {})
                            if not source or not isinstance(source, dict):
                                logger.warning(f"Skipping message Offset={message.offset} due to missing/invalid source info. Payload keys: {list(payload.keys())}")
                                continue
    
                            # DEBUG: Log source structure
                            logger.debug(f"Message Offset={message.offset} Source Info: {source}")
    
                            connector_type = source.get('connector', 'unknown')
                            db_name = source.get('db', 'unknown_db')
                            schema_name = source.get('schema', 'unknown_schema') # schema might be null for MySQL source block
                            table_name = source.get('table', 'unknown_table')
    
                            # INFO: Log extracted source details
                            logger.info(f"Message Offset={message.offset}: Connector='{connector_type}', DB='{db_name}', Schema='{schema_name}', Table='{table_name}'")
    
                            # --- Check if it's MySQL but we don't see output ---
                            if connector_type == 'mysql':
                                logger.info(f"MYSQL message Offset={message.offset} detected. Proceeding to format and send output...")
    
                            # --- Existing Formatting and Sending Logic ---
                            try:
                                # (This part extracts field types, formats timestamps, builds identifiers etc.)
                                field_types = extract_field_types_from_schema(message_schema) # Errors here?
                                db_type_display = "PostgreSQL" if connector_type == 'postgresql' else "MySQL" if connector_type == 'mysql' else connector_type.capitalize()
                                table_identifier = f"{db_name}"
                                if schema_name and schema_name != db_name: table_identifier += f".{schema_name}" # MySQL schema might be null
                                table_identifier += f".{table_name}"
                                operation = {'c': 'INSERT', 'u': 'UPDATE', 'd': 'DELETE', 'r': 'READ (Snapshot)'}.get(op_type, f'UNKNOWN ({op_type})')
                                op_style = {'INSERT': STYLE_OP_INSERT, 'UPDATE': STYLE_OP_UPDATE, 'DELETE': STYLE_OP_DELETE, 'READ (Snapshot)': STYLE_OP_READ}.get(operation, STYLE_OP_UNKNOWN)
                                event_ts_ms = source.get('ts_ms')
                                event_time_str = format_timestamp_ms(event_ts_ms) if event_ts_ms else "N/A" # Errors here?
    
                                # --- Send Message Header to GUI ---
                                logger.debug(f"Sending header for Offset={message.offset} to GUI callback.")
                                send_output(gui_callback, "\n" + "="*80 + "\n", STYLE_SEPARATOR)
                                # ... (rest of send_output calls for header) ...
                                send_output(gui_callback, f"{db_type_display} Event: ", STYLE_HEADER)
                                send_output(gui_callback, f"{operation}", op_style)
                                send_output(gui_callback, f" on ", STYLE_HEADER)
                                send_output(gui_callback, f"{table_identifier}\n", STYLE_HEADER)
                                send_output(gui_callback, f"Source Timestamp: ", STYLE_TIMESTAMP)
                                send_output(gui_callback, f"{event_time_str}", STYLE_TIMESTAMP)
                                send_output(gui_callback, f" | Kafka Offset: {message.offset}\n", STYLE_NORMAL)
                                send_output(gui_callback, "-"*80 + "\n", STYLE_SEPARATOR)
    
    
                                # --- Send Diffs to GUI ---
                                logger.debug(f"Sending diff for Offset={message.offset} to GUI callback.")
                                print_colored_diff(before_data, after_data, field_types, gui_callback) # Errors inside here?
    
                                send_output(gui_callback, "="*80 + "\n", STYLE_SEPARATOR)
                                logger.info(f"Successfully processed and sent message Offset={message.offset}")
    
                            except Exception as format_exc:
                                # Catch errors specifically during the formatting/sending part
                                logger.error(f"Error during formatting/sending message Offset={message.offset}: {format_exc}\n{traceback.format_exc()}")
                                send_output(gui_callback, f"ERROR formatting/displaying message Offset={message.offset}: {format_exc}\n", STYLE_ERROR)
                                # Continue to next message
    
                # --- Outer Exception Handling for Poll/Batch ---
                except json.JSONDecodeError as e:
                    # This error affects how messages are read by poll, might affect subsequent messages
                    logger.error(f"Failed to decode JSON message value during poll: {e}")
                    send_output(gui_callback, f"ERROR: Failed to decode message batch: {e}\n", STYLE_ERROR)
                except KafkaErrors.CommitFailedError as e:
                     logger.error(f"Offset commit failed: {e}. May re-process messages.")
                     send_output(gui_callback, f"WARNING: Offset commit failed: {e}\n", STYLE_ERROR)
                except Exception as e:
                    # Catch errors during the poll() or batch iteration itself
                    logger.error(f"Error processing message batch: {e}\n{traceback.format_exc()}")
                    send_output(gui_callback, f"ERROR processing message batch: {e}\n", STYLE_ERROR)
    
            # --- End of Inner Loop (Stop Event) ---
            logger.info("Stop signal received, exiting consumer loop.")
    
        # --- Outer Exception Handling for Connection/Setup ---
        except KafkaErrors.NoBrokersAvailableError as e:
            # ... (error handling unchanged) ...
            logger.error(f"Cannot connect to Kafka brokers at {config.CONSUMER_BOOTSTRAP_SERVERS}: {e}. Retrying in 10 seconds...")
            if listening_message_sent: send_output(gui_callback, f"ERROR: Cannot connect to Kafka ({e}). Retrying...\n", STYLE_ERROR)
            if consumer:
                try: consumer.close(timeout=1)
                except: pass
                finally: consumer = None
            time.sleep(10)
        except KafkaErrors.GroupAuthorizationFailedError as e:
            # ... (error handling unchanged) ...
             logger.error(f"Kafka group authorization failed for group '{config.CONSUMER_GROUP_ID}': {e}. Stopping.")
             send_output(gui_callback, f"FATAL: Kafka group authorization failed: {e}\n", STYLE_ERROR)
             break
        except ValueError as e:
             # ... (error handling unchanged) ...
             logger.error(f"Configuration or compatibility error connecting to Kafka: {e}. Stopping.")
             send_output(gui_callback, f"FATAL: Kafka configuration error: {e}\n", STYLE_ERROR)
             break
        except Exception as e:
            # ... (error handling unchanged) ...
            logger.error(f"Unexpected error in Kafka consumer setup/loop: {e}\n{traceback.format_exc()}")
            send_output(gui_callback, f"FATAL: Unexpected consumer error: {e}\n", STYLE_ERROR)
            if consumer:
                try: consumer.close(timeout=1)
                except: pass
                finally: consumer = None
            time.sleep(15)
    
    # --- Cleanup ---
    if consumer:
        try:
            logger.info("Closing Kafka consumer (timeout 5s)...")
            consumer.close(timeout=5)
            logger.info("Kafka consumer closed successfully.")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")

    # Send final status to GUI
    send_output(gui_callback, "\n--- CONSUMER STOPPED ---\n", STYLE_HEADER)
    logger.info("Consumer thread finished execution.")