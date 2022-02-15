from confluent_kafka import Producer
import sys

msg = str(sys.argv[1])

def run():

    try:
        producer = Producer(
            {
                "bootstrap.servers": "localhost:9093, localhost:9094, localhost:9095",
            }
        )

        def message() -> dict:

            if msg[0] < "N":
                partition = 0
            else:
                partition = 1

            return{
                "value" : msg,
                "partition" : partition
            }

        message_sent = message()

        producer.poll(0)
        result = producer.produce(
            "Users",
            value = str(msg)
        )

        print("Done! Created Successfully! " + str(message_sent))
        producer.flush()

    except Exception as e:
        print("Something bad happened " + str(e))

    finally:
        sys.exit()

run()
