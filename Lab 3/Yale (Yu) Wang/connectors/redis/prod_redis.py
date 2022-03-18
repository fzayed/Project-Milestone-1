from kafka import KafkaProducer
import json
import time
import io
from avro.io import DatumWriter, BinaryEncoder
import numpy as np
import os

data=json.load(open('cred.json'))
bootstrap_servers=data['bootstrap_servers']
sasl_plain_username=data['Api key']
sasl_plain_password=data['Api secret']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,key_serializer=lambda v: v.encode())
    
fileNum=0
for filename in os.scandir("./Cam0"):
	if filename.is_file():
		with open(filename.path, "rb") as f:
			value = f.read()
			print(f'file {filename.name}_pic{fileNum}')
			producer.send('ToRedis', value,key=filename.name+'_pic'+str(fileNum))
			fileNum+=1

producer.close()
