#!/usr/bin/env python
# encoding: utf-8

import ssl
from kafka import KafkaProducer
from kafka.errors import KafkaError
import setting
import datetime
import socket
import json
import argparse

# Erstelle einen Argumentparser
parser = argparse.ArgumentParser(description='Zahlenparameter von der Kommandozeile lesen und ausgeben.')
parser.add_argument('number', type=int, help='Eine ganze Zahl, die von der Kommandozeile gelesen wird')

# Lese die Argumente von der Kommandozeile
args = parser.parse_args()

conf = setting.kafka_setting

print("conf:", conf)

context = ssl.create_default_context()
context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
## The new python(2.7.8+) may cannot ignore the hostname check,
## you could set to ssl.CERT_NONE to walk around the problem,
## or you can change the client to confluent-python-demo 

#context.verify_mode = ssl.CERT_NONE
context.verify_mode = ssl.CERT_REQUIRED

context.check_hostname = False
context.load_verify_locations("mix-4096-ca-cert")

producer = KafkaProducer(bootstrap_servers=conf['bootstrap_servers'],
                         sasl_mechanism="PLAIN",
                         ssl_context=context,
                         security_protocol='SASL_SSL',
                         api_version=(0, 10),
                         retries=5,
                         sasl_plain_username=conf['sasl_plain_username'],
                         sasl_plain_password=conf['sasl_plain_password'])

partitions = producer.partitions_for(conf['topic_name'])
print('Topic: %s' % partitions)

for i in range(args.number):
#    print(f'Das ist Ausgabe Nummer {i + 1}')

#    payload = input("Input payload: ")
    payload = f"data {i + 1}"
    timestamp = datetime.datetime.now().isoformat()
    local_ip = socket.gethostbyname(socket.gethostname())

    data = {
        "timestamp": timestamp,
        "local IP": local_ip,
        "payload": payload
    }
    json_data = json.dumps(data)
    
    try:
        future = producer.send(conf['topic_name'], json_data.encode())
        future.get()
        print('send message succeed.')
    except KafkaError:
        print('send message failed.')
        print(KafkaError)
