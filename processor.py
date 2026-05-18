from kafka import KafkaConsumer, KafkaProducer
from const import BROKER_ADDR, BROKER_PORT
from collections import deque
import json

WINDOW_SIZE = 5  #moving average over last 5 readings

consumer = KafkaConsumer(
    'topic_sensor',
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    value_deserializer=lambda v: json.loads(v.decode()),
    auto_offset_reset='earliest'
)
producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    value_serializer=lambda v: json.dumps(v).encode()
)

window = deque(maxlen=WINDOW_SIZE)
print(f'Processor started. Consuming topic_sensor, publishing to topic_processed...')
try:
    for msg in consumer:
        reading = msg.value
        window.append(reading['value'])
        average = round(sum(window) / len(window), 2)
        processed = {'timestamp': msg.value['timestamp'], 'value': msg.value['value'], 'average': average}
        producer.send('topic_processed', value=processed)
        producer.flush()
        print(f'Processed: {processed}')
except KeyboardInterrupt:
    print('Processor stopped.')