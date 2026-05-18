import grpc
import temperature_pb2, temperature_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')
stub = temperature_pb2_grpc.TemperatureServiceStub(channel)

print('=== Última leitura ===')
r = stub.GetLatest(temperature_pb2.Empty())
print(f'{r.timestamp}: {r.value}°C')

print('\n=== Histórico (últimas 5) ===')
history = stub.GetHistory(temperature_pb2.HistoryRequest(limit=5))
for r in history.records:
    print(f'  {r.timestamp}: {r.value}°C')

print('\n=== Média geral ===')
avg = stub.GetAverage(temperature_pb2.Empty())
print(f'Média: {avg.average}°C sobre {avg.count} leituras')