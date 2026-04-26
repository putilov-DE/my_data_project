import os
import json
import csv
from datetime import datetime
import time
from confluent_kafka import Consumer, KafkaError

# Settings
KAFKA_BROKER = 'kafka-server:9092'
TOPIC = 'tweets'
GROUP_ID = 'tweet_processors'
OUTPUT_DIR = '/app/output_data'  # Path inside the container

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

print(f"Consumer started. Watching for messages in topic '{TOPIC}'...")

# Message buffer: { "dd_mm_yyyy_hh_mm": [tweet1, tweet2] }
buffer = {}
last_dump_time = time.time()


def dump_buffer_to_csv():
    global buffer
    if not buffer:
        return

    for time_key, tweets in buffer.items():
        filename = f"{OUTPUT_DIR}/tweets_{time_key}.csv"
        file_exists = os.path.isfile(filename)

        with open(filename, 'a', newline='', encoding='utf-8') as f:
            # Keep only the fields required by the assignment
            keys = ['author_id', 'created_at', 'text']
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')

            if not file_exists:
                writer.writeheader()
            writer.writerows(tweets)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Saved {len(tweets)} tweets to {filename}")

    buffer.clear()  # Clear the buffer after writing


try:
    while True:
        msg = consumer.poll(1.0)

        # Check if it's time to flush the buffer to disk (every 60 seconds)
        current_time = time.time()
        if current_time - last_dump_time >= 60:
            dump_buffer_to_csv()
            last_dump_time = current_time

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            continue

        # Parse JSON
        raw_data = json.loads(msg.value().decode('utf-8'))

        # Extract tweet creation time (expected format: "2022-05-03 17:08:45")
        created_at_str = str(raw_data.get('created_at', ''))

        try:
            dt_obj = datetime.fromisoformat(created_at_str)
            time_key = dt_obj.strftime("%d_%m_%Y_%H_%M")
        except ValueError:
            time_key = "unknown_time"

        # Keep only the required fields
        tweet_data = {
            'author_id': raw_data.get('author_id', ''),
            'created_at': raw_data.get('created_at', ''),
            'text': raw_data.get('text', '')
        }

        # Add to buffer
        if time_key not in buffer:
            buffer[time_key] = []
        buffer[time_key].append(tweet_data)

except KeyboardInterrupt:
    pass
finally:
    # Save remaining data on shutdown
    dump_buffer_to_csv()
    consumer.close()
    print("Consumer gracefully shut down.")