from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

while True:
    msg = input("Enter message to send (or 'exit' to quit): ")
    if msg.lower() == 'exit':
        break
    producer.produce('test-topic', key='key1', value=msg, callback=delivery_report)#this is asynchrous
    producer.flush()#ensures each message is actually delivered before promoting for next input.

#Asynchronous means "fire and forget", then get notified later.
#Synchronous means "wait until done" before moving on.

#synchrous code portion below
#producer.produce('topic', key='key', value='message')
#producer.flush()  # blocks until message is confirmed sent
