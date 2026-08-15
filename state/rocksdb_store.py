from rocksdict import Rdict
DB_PATH = "data/rocksdb"
class RocksDBStateStore:
    """RocksDB-backed state store for truck processing."""

    def __init__(self, db_path=DB_PATH):
        self.db = Rdict(db_path)

    def put(self, key, value):
        self.db[key] = value

    def get(self, key):
        return self.db.get(key)

    def delete(self, key):
        if key in self.db:
            del self.db[key]

    def close(self):
        self.db.close()