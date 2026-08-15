from rocksdb_store import RocksDBStateStore


DB_PATH = "data/recovery_test"


# -----------------------------
# Step 1: Save state
# -----------------------------

store = RocksDBStateStore(DB_PATH)

store.put(
    "truck:1",
    {
        "temperature_sum": 96.0,
        "reading_count": 3,
        "window_start": "2026-08-15T10:00:00"
    }
)

print("State saved:")
print(store.get("truck:1"))

store.close()


# -----------------------------
# Step 2: Simulate restart
# -----------------------------

print("\nSimulating worker restart...")

store = RocksDBStateStore(DB_PATH)


# -----------------------------
# Step 3: Recover state
# -----------------------------

recovered_state = store.get("truck:1")

print("Recovered state:")
print(recovered_state)


# -----------------------------
# Step 4: Verify recovery
# -----------------------------

if recovered_state is not None:
    print("\nSUCCESS: State recovered from RocksDB.")
else:
    print("\nFAILED: State was not recovered.")


store.close()