import pyarrow.flight as flight

client = flight.FlightClient("grpc+tcp://[::1]:31010")
flights = client.list_flights()
for f in flights:
    print(f.descriptor)
