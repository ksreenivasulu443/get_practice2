from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])

print("Waiting for messages... (press Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print(f"Received: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Exiting...")
finally:
    consumer.close()
