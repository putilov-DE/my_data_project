import json
import time
import requests
from sseclient import SSEClient
from confluent_kafka import Producer

# Kafka configuration using the container name defined in docker-compose
conf = {
    'bootstrap.servers': 'kafka-server:9092',
    'client.id': 'wiki-producer'
}

producer = Producer(conf)
KAFKA_TOPIC = 'input'
WIKI_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/page-create'


def delivery_report(err, msg):
    """
    Callback called once for each message produced to indicate delivery result.
    """
    if err is not None:
        print(f"[-] Delivery failed: {err}")
    else:
        print(f"[+] Delivered to {msg.topic()} [{msg.partition()}]")


def main():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/event-stream'
    }

    # Infinite loop to automatically reconnect if the server drops the connection
    while True:
        try:
            print(f"\n[*] Connecting to Wikipedia stream: {WIKI_STREAM_URL} ...")
            response = requests.get(WIKI_STREAM_URL, stream=True, headers=headers)
            response.raise_for_status()

            client = SSEClient(response)
            print("[*] Connected! Listening for page creations...")

            for event in client.events():
                if event.event == 'message':
                    # Optional: uncomment to see raw data length
                    # print(f"-> Got message! Length: {len(event.data)}")
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        value=event.data.encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.poll(0)

        # Catch specific connection drop error
        except requests.exceptions.ChunkedEncodingError:
            print("\n[!] Connection dropped by Wikipedia server (ChunkedEncodingError).")
            print("[*] Reconnecting in 5 seconds...")
            time.sleep(5)

        # Catch any other generic errors (network offline, etc.)
        except Exception as e:
            print(f"\n[!] Stream error: {e}")
            print("[*] Reconnecting in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()