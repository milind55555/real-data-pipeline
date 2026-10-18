"""
IoT Sensor Data Producer for Kafka.

Simulates 1000 smart meters sending telemetry (voltage, current, power, temperature)
to the 'iot_events' topic at ~500 events/sec. Run with:
    pip install kafka-python
    python kafka/producer.py
"""
import json
import time
import random
import signal
import sys
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "iot_events"
NUM_METERS = 1000
EVENTS_PER_SECOND = 500

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",                       # strongest durability guarantee
    compression_type="snappy",        # reduces network + storage
    linger_ms=5,                      # small batching for throughput
    retries=10,                       # transient error handling
    max_in_flight_requests_per_connection=5,
)

running = True


def shutdown_handler(signum, frame):
    global running
    running = False
    print("\nShutting down producer...")


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


def generate_event(device_id: int) -> dict:
    """Simulate a realistic smart-meter reading."""
    return {
        "device_id": f"meter-{device_id:05d}",
        "timestamp_ms": int(time.time() * 1000),
        "voltage_v": round(random.gauss(230, 5), 2),          # nominal 230V
        "current_a": round(random.gauss(10, 3), 2),           # nominal 10A
        "power_w": round(random.gauss(2300, 300), 2),         # V*I
        "temperature_c": round(random.gauss(45, 8), 2),       # device temp
        "grid_frequency_hz": round(random.gauss(50, 0.1), 3), # 50 Hz grid
    }


def main():
    print(f"Producing to {TOPIC} on {BOOTSTRAP_SERVERS}")
    print(f"Devices: {NUM_METERS} | Target: {EVENTS_PER_SECOND} events/sec")
    print("Press Ctrl+C to stop.\n")

    interval = 1.0 / EVENTS_PER_SECOND
    next_send = time.time()
    count = 0

    while running:
        device_id = random.randint(0, NUM_METERS - 1)
        event = generate_event(device_id)
        producer.send(TOPIC, value=event)
        count += 1

        if count % 1000 == 0:
            print(f"Sent {count} events...")

        # pace to target rate
        next_send += interval
        sleep_for = next_send - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)

    producer.flush(timeout=30)
    print(f"\nDone. Total events sent: {count}")


if __name__ == "__main__":
    main()