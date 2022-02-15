from confluent_kafka import Producer, KafkaError
import sys

msg = sys.argv[1]
msgBytes = str.encode(sys.argv[1])

def error_cb(error):
    print(error)

def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

producer = Producer({
    'bootstrap.servers': 'pkc-4yyd6.us-east1.gcp.confluent.cloud:9092',
    'auto.offset.reset': 'earliest',
    'group.id' : 'myGroupId',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism':   'PLAIN',
    'sasl.username':  'MZ7UHXE3CUCBP4GJ',
    'sasl.password':  'G4jyIts0C+6rLklccDrMRArbHhYN0Mxo9GVVP2Z1yFPNG265ZMaxSyMuj/V4PyRT',
    'error_cb': error_cb
})

producer.produce("topic", msg, callback=delivery_callback)

producer.flush()