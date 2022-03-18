from kafka import KafkaProducer
import json
import time
import io
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import csv

schemaID=100010

data=json.load(open('cred.json'))
bootstrap_servers=data['bootstrap_servers']
sasl_plain_username=data['Api key']
sasl_plain_password=data['Api secret']

schema = avro.schema.parse(open("./schema.avsc").read())
writer = DatumWriter(schema)

def encode(value):
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    return schemaID.to_bytes(5, 'big')+bytes_writer.getvalue()

with open('kvh.csv') as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',')
	line_count = 0
	id_counter = 0
	for row in csv_reader:
		if line_count > 10:
			break
		print(f'\t{row[0]} time, {row[1]} value.')
		line_count += 1
		value=dict(id=id_counter,time=row[0],camera_b=float(row[1]),modified=int(1000*time.time()))
		id_counter += 1
		producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
		    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,value_serializer=lambda m: encode(m))
		producer.send('KVH', value)
	print(f'Processed {line_count} lines.')

producer.close()
