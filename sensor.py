from kafka import KafkaProducer
from const import BROKER_ADDR, BROKER_PORT
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    value_serializer=lambda v: json.dumps(v).encode()
)

temperature = 25.0
while True:
    variation = random.uniform(-2.0, 2.0)
    temperature = round(temperature + variation, 2)
    event = {'timestamp': time.strftime('%Y-%m-%dT%H:%M:%S'), 'value': temperature}
    producer.send('topic_sensor', value=event)
    print(f'Published: {event}')
    time.sleep(2)