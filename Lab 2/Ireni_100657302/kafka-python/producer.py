from kafka import KafkaProducer
import sys

msg = str(sys.argv[1])

def run():

    try:

        producer = KafkaProducer(
            bootstrap_servers = ['localhost:9093','localhost:9094','localhost:9095']
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

        result = producer.send(
            "Users",
            bytes(str(msg), 'utf-8')
        )

        print("Done! Created Successfully! " + str(message_sent))
        producer.flush()

        producer.close()

    except Exception as e:
        print("Something bad happened " + str(e))


    finally:
        sys.exit()

run()
