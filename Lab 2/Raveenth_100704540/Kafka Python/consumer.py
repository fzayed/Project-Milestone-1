from asyncio.windows_events import NULL
from ensurepip import bootstrap
from kafka import KafkaConsumer

try:
    consumer = KafkaConsumer(
        group_id=None,
        auto_offset_reset='earliest',
        bootstrap_servers= [
            "localhost:9093",
            "localhost:9094",
            "localhost:9095"
        ]
    )

    consumer.subscribe(topics="topic")
    print("Consumer is connected, awaiting messages...")

    for message in consumer:
        print("Topic: %s Partition: %d Message: %s" % (message.topic, message.partition, message.value))

    # KafkaConsumer(consumer_timeout_ms=1000)

except Exception as e:
    print("Consumer cannot connect: ", e.__class__)