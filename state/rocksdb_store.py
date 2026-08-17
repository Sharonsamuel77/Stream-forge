from rocksdict import Rdict
from pathlib import Path


BASE_DB_PATH = "data/rocksdb"


class RocksDBStateStore:
    """RocksDB-backed state store scoped to a Kafka partition."""

    def __init__(self, partition):
        self.partition = partition

        db_path = Path(BASE_DB_PATH) / f"partition_{partition}"
        db_path.mkdir(parents=True, exist_ok=True)

        self.db_path = str(db_path)
        self.db = Rdict(self.db_path)

    def put(self, key, value):
        self.db[key] = value

    def get(self, key):
        return self.db.get(key)

    def delete(self, key):
        if key in self.db:
            del self.db[key]

    def close(self):
        self.db.close()