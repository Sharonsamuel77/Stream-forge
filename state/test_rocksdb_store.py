from rocksdb_store import RocksDBStateStore


store = RocksDBStateStore("data/test_rocksdb")

store.put(
    "truck_1",
    {
        "temperature_sum": 150.0,
        "reading_count": 5
    }
)

print("Stored:", store.get("truck_1"))

store.put(
    "truck_1",
    {
        "temperature_sum": 180.0,
        "reading_count": 6
    }
)

print("Updated:", store.get("truck_1"))

store.close()

print("RocksDB test completed.")