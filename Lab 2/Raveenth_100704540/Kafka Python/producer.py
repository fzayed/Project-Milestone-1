from kafka import KafkaProducer
import sys

msg = sys.argv[1]
msgBytes = str.encode(sys.argv[1])
partition = 0 if msg[0] < "N"  else 1

try:
    producer = KafkaProducer(
        client_id='myapp',
        bootstrap_servers= [
            "localhost:9093",
            "localhost:9094",
            "localhost:9095"
        ]
    )

    producer.send('topic', value=msgBytes, partition=partition)
    print("Message successfully sent!")

    producer.flush()

except Exception as e:
    print("Producer could not send message: ", e.__class__)

finally:
    sys.exit()