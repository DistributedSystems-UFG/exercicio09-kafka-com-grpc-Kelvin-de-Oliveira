from kafka import KafkaConsumer
from const import BROKER_ADDR, BROKER_PORT, GRPC_PORT
import json
import sqlite3
import threading
import grpc
from concurrent import futures
import temperature_pb2
import temperature_pb2_grpc

DB_FILE = 'temperatures.db'

def init_db():
    con = sqlite3.connect(DB_FILE)
    con.execute('CREATE TABLE IF NOT EXISTS readings (timestamp TEXT, value REAL, average REAL)')
    con.commit()
    con.close()

def save_reading(record):
    con = sqlite3.connect(DB_FILE)
    con.execute('INSERT INTO readings VALUES (?,?,?)', (record['timestamp'], record['value'], record['average']))
    con.commit()
    con.close()

#thread que consome o kafka continuamente
def kafka_consumer_thread():
    consumer = KafkaConsumer(
        'topic_processed',
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset='earliest'
    )
    for msg in consumer:
        save(msg.value)
        print(f'Saved to DB:  {msg.value}')

#endpoints gRPC
class TemperatureServicer(temperature_pb2_grpc.TemperatureServiceServicer):
    def GetLatest(self, request, context):
        con = sqlite3.connect(DB_FILE)
        row = con.execute('SELECT timestamp, value FROM readings ORDER BY rowid DESC LIMIT 1').fetchone()
        con.close()
        if row:
            return temperature_pb2.TemperatureRecord(timestamp=row[0], value=row[1])
        return temperature_pb2.TemperatureRecord(timestamp='N/A', value=0.0)

    def GetHistory(self, request, context):
        limit = request.limit if request.limit > 0 else 10
        con = sqlite3.connect(DB_FILE)
        rows = con.execute('SELECT timestamp, value FROM readings ORDER BY rowid DESC LIMIT ?', (limit,)).fetchall()
        con.close()
        records = [temperature_pb2.TemperatureRecord(timestamp=r[0], value=r[1]) for r in rows]
        return temperature_pb2.TemperatureList(records=records)

    def GetAverage(self, request, context):
        con = sqlite3.connect(DB_FILE)
        row = con.execute('SELECT AVG(average), COUNT(*) FROM readings').fetchone()
        con.close()
        return temperature_pb2.AverageRecord(average=round(row[0] or 0, 2), count=row[1])

init_db()
threading.Thread(target=kafka_consumer_thread, daemon=True).start()

grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
temperature_pb2_grpc.add_TemperatureServiceServicer_to_server(TemperatureServicer(), grpc_server)
grpc_server.add_insecure_port('[::]:50051')
grpc_server.start()
print('gRPC server running on port 50051')
grpc_server.wait_for_termination()