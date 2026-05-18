from const import GRPC_HOST, GRPC_PORT
import grpc
import temperature_pb2
import temperature_pb2_grpc

channel = grpc.insecure_channel(f'{GRPC_HOST}:{GRPC_PORT}')
stub = temperature_pb2_grpc.TemperatureServiceStub(channel)

print('=' * 45)
print(' Temperature Service — gRPC Client')
print('=' * 45)


print('\n[1] Latest reading:')
latest = stub.GetLatest(temperature_pb2.Empty())
print(f'    {latest.timestamp}  →  {latest.value:.2f} °C')


print('\n[2] Last 5 readings:')
history = stub.GetHistory(temperature_pb2.HistoryRequest(limit=5))
if history.records:
    for r in history.records:
        print(f'    {r.timestamp}  →  {r.value:.2f} °C')
else:
    print('    (no data yet)')

print('\n[3] Overall average:')
avg = stub.GetAverage(temperature_pb2.Empty())
print(f'    {avg.average:.2f} °C  over  {avg.count} readings')