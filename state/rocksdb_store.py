from rocksdict import Rdict

DB_PATH = "data/rocksdb"


class RocksDBStateStore:
    """RocksDB-backed state store for truck processing."""

    def __init__(self, db_path=DB_PATH):

        print("Loading state from RocksDB...")

        self.db = Rdict(db_path)

        count = sum(1 for _ in self.db.items())

        print(f"Recovered {count} records from RocksDB")

    def put(self, key, value):
        self.db[key] = value

    def get(self, key):
        return self.db.get(key)

    def delete(self, key):
        if key in self.db:
            del self.db[key]

    def all(self):
        return list(self.db.items())

    def count(self):
        return len(self.db)

    def recovery_info(self):
        return {
            "status": "Recovered",
            "state_store": "RocksDB",
            "records": len(self.db)
        }

    def close(self):
        self.db.close()