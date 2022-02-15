from kafka import KafkaConsumer
import json, sys


def run():

    try:

        consumer = KafkaConsumer(
            "Users",             # topic name, brokers
            auto_offset_reset='earliest',
            bootstrap_servers=['localhost:9093',
                               'localhost:9094', 'localhost:9095']
        )

        consumer.subscribe(['Users'])

#         infinite loop
        for msg in consumer:
            print(msg)

    except Exception as e:
        print("Something bad happened " + str(e))

    except KeyboardInterrupt as e:
        sys.exit(0)

run()
