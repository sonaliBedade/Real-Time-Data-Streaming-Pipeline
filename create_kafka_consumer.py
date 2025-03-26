from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timezone
from collections import defaultdict, Counter
import time
import psutil  # For resource monitoring

# Kafka configurations
BOOTSTRAP_SERVERS = "localhost:29092"
SOURCE_TOPIC = "user-login"
DESTINATION_TOPIC = "processed-user-login"

# Initialize Kafka consumer
consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id="user-login-group",  # Add consumer group for scaling
    auto_offset_reset="earliest"  # Start consuming from the earliest message
)

# Initialize Kafka producer with Snappy compression
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks="all",  # Ensure messages are persisted before acknowledgment
    batch_size=16384,  # Optimize batch size for performance
    linger_ms=5,  # Wait time before sending a batch of messages
    compression_type="snappy"  # Enable Snappy compression
)

# Tracking logins per app version, user IPs, locales, and devices
app_version_counts = defaultdict(int)
user_ip_history = defaultdict(set)
locale_counts = defaultdict(int)
device_usage = defaultdict(set)  # Tracks unique users per device ID
device_type_counter = Counter()  # Tracks most used device types

# Error handling function
def handle_error(error_message):   # Log errors and continue processing
    print(f"ERROR: {error_message}")

# Resource usage tracking function
def get_system_metrics():   # Get current CPU and memory usage.
    cpu = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory().percent
    return cpu, memory

# Processing Function
def process_message(message):   # Apply transformations to the consumed message with error handling.
    try:
        processed_data = {}

        # Extract fields with default values
        user_id = message.get("user_id", None)
        if not user_id:
            handle_error("Missing user_id")
            return None

        app_version = message.get("app_version", "unknown")
        device_type = message.get("device_type", "unknown").lower()
        ip = message.get("ip", "unknown")
        locale = message.get("locale", "unknown")
        device_id = message.get("device_id", "unknown")
        timestamp = message.get("timestamp", None)

        if timestamp is None:
            handle_error("Missing timestamp")
            return None

        # Step 1: Filter only Android and iOS devices
        if device_type not in ["android", "ios"]:
            return None  # Skip non-mobile devices

        # Step 2: Aggregate logins per app version
        app_version_counts[app_version] += 1

        # Step 3: Detect suspicious login behavior (Multiple logins from different IPs per user)
        is_suspicious = ip in user_ip_history[user_id]
        user_ip_history[user_id].add(ip)

        # Step 4: Normalize timestamp (Convert Unix timestamp to human-readable format)
        try:
            normalized_timestamp = datetime.fromtimestamp(int(timestamp), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            handle_error(f"Invalid timestamp: {timestamp}")
            normalized_timestamp = "invalid_timestamp"

        # Step 5: Geo-based analysis (Tracking logins per locale)
        locale_counts[locale] += 1
        logs_from_multiple_locations = len(user_ip_history[user_id]) > 1  # User has logged in from different IPs

        # Device ID-based analysis: Detect shared devices
        device_usage[device_id].add(user_id)
        shared_device = len(device_usage[device_id]) > 1  # Multiple users logging in from the same device

        # Track device type popularity
        device_type_counter[device_type] += 1
        most_common_device_type = device_type_counter.most_common(1)[0][0] if device_type_counter else "unknown"

        # Compile processed data
        processed_data.update({
            "user_id": user_id,
            "app_version": app_version,
            "total_logins_for_version": app_version_counts[app_version],
            "ip": ip,
            "suspicious_login": is_suspicious,
            "logs_from_multiple_locations": logs_from_multiple_locations,
            "normalized_timestamp": normalized_timestamp,
            "locale": locale,
            "total_logins_from_locale": locale_counts[locale],
            "device_id": device_id,
            "shared_device": shared_device,
            "device_type": device_type,
            "most_common_device_type": most_common_device_type
        })

        return processed_data

    except Exception as e:
        handle_error(f"Error processing message: {e}")
        return None

# Benchmarking variables
start_time = time.time()
message_count = 0
latency_sum = 0  # Track latency for throughput calculation
error_count = 0

# Shutdown and Error Handling
print("Starting Kafka Consumer... Press Ctrl+C to stop.")

try:
    for message in consumer:
        start_msg_time = time.time()  # Track message processing start time
        
        raw_data = message.value
        processed_data = process_message(raw_data)

        if processed_data:
            producer.send(DESTINATION_TOPIC, value=processed_data)
            print(f"Processed and sent: {processed_data}")
            message_count += 1  # Count processed message
            latency_sum += (time.time() - start_msg_time)  # Measure latency for each message

        # Every 100 messages, print benchmark stats
        if message_count % 100 == 0:
            elapsed_time = time.time() - start_time
            throughput = message_count / elapsed_time
            avg_latency = latency_sum / message_count if message_count > 0 else 0
            cpu, memory = get_system_metrics()

            print(f"Throughput: {throughput:.2f} msgs/sec")
            print(f"Avg Latency: {avg_latency:.4f} secs")
            print(f"CPU Usage: {cpu}% | Memory Usage: {memory}%")
            print(f"Errors: {error_count} | Processed Messages: {message_count}")

except KeyboardInterrupt:
    print("\nShutting down Kafka Consumer...")

except Exception as e:
    handle_error(f"Unexpected error: {e}")

finally:
    consumer.close()
    producer.close()
    print("Kafka Consumer closed.")
