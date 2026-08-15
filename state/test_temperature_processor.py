from rocksdb_store import RocksDBStateStore
from temperature_processor import TemperatureProcessor


store = RocksDBStateStore("data/test_processor")
processor = TemperatureProcessor(store)


events = [
    ("truck_1", 30.0, "2026-08-15T10:00:00"),
    ("truck_1", 32.0, "2026-08-15T10:01:00"),
    ("truck_1", 34.0, "2026-08-15T10:02:00"),
    ("truck_1", 36.0, "2026-08-15T10:05:00"),

    ("truck_2", 25.0, "2026-08-15T10:00:00"),
    ("truck_2", 27.0, "2026-08-15T10:01:00"),
    ("truck_2", 29.0, "2026-08-15T10:02:00"),
    ("truck_2", 31.0, "2026-08-15T10:05:00"),
]


for truck_id, temperature, timestamp in events:
    result = processor.process(
        truck_id,
        temperature,
        timestamp
    )

    print(
        f"Processed | "
        f"truck={truck_id} | "
        f"temperature={temperature} | "
        f"result={result}"
    )


store.close()

print("Temperature processor test completed.")