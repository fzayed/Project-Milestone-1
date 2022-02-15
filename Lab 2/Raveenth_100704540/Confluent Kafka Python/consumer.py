from confluent_kafka import Consumer, KafkaError
 
def error_cb(error):
    print(error)
 
c = Consumer({
     'bootstrap.servers': 'pkc-4yyd6.us-east1.gcp.confluent.cloud:9092',
     'auto.offset.reset': 'earliest',
     'group.id' : 'myGroupId',
     'security.protocol': 'SASL_SSL',
     'sasl.mechanism':   'PLAIN',
     'sasl.username':  'MZ7UHXE3CUCBP4GJ', # API Key
     'sasl.password':  'G4jyIts0C+6rLklccDrMRArbHhYN0Mxo9GVVP2Z1yFPNG265ZMaxSyMuj/V4PyRT', # Secret Key
     'error_cb': error_cb
})
 
c.subscribe(['topic'])
 
while True:
    msg = c.poll(10.0)
 
    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
 
    print('Received message: {}'.format(msg.value().decode('utf-8')))
 
c.close()