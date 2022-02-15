from confluent_kafka import Consumer
import json, sys

def run():

    try:

        consumer = Consumer({
            'group.id' : 'cloud-computing',
            'auto.offset.reset' : 'earliest',
            'bootstrap.servers' : 'localhost:9093,localhost:9094,localhost:9095'}
        )

        consumer.subscribe(['Users'])

#         infinite loop
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer Error")
            print("Received: {}".format(msg.value().decode('utf-8')))

    except Exception as e:
        print("Something bad happened " + str(e))

    except KeyboardInterrupt as e:
        sys.exit(0)

run()
