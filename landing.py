"""Single-table immutable fragments. No cross-table visibility policy."""
import hashlib
import json
import sqlite3

TABLES = ("samples", "qc")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Table:
    def __init__(self, name):
        if name not in TABLES:
            raise ValueError("unknown table")
        self.name = name
        self.db = sqlite3.connect(":memory:")
        self.db.executescript("""
            CREATE TABLE fragments(tid TEXT PRIMARY KEY, seq INTEGER, digest TEXT);
            CREATE TABLE versions(tid TEXT, seq INTEGER, key TEXT, payload TEXT,
                                  PRIMARY KEY(tid, key));
        """)

    def land(self, tid, seq, rows):
        if not isinstance(rows, list):
            raise ValueError("rows must be a list")
        seen = set()
        for row in rows:
            if not isinstance(row, list) or len(row) != 2:
                raise ValueError("row must be [key, value]")
            key, value = row
            if not isinstance(key, str) or not key or key in seen:
                raise ValueError("duplicate or invalid key")
            seen.add(key)
            if self.name == "samples":
                valid = value is None or type(value) is int
            else:
                valid = value is None or value in ("ok", "bad")
            if not valid:
                raise ValueError("invalid value")
        body = canonical(sorted(rows, key=lambda row: row[0]))
        digest = hashlib.sha256(body.encode()).hexdigest()
        old = self.db.execute(
            "SELECT seq,digest FROM fragments WHERE tid=?", (tid,)).fetchone()
        if old is not None:
            if old != (seq, digest):
                raise ValueError("fragment conflict")
            return (tid, self.name, digest)
        with self.db:
            self.db.execute("INSERT INTO fragments VALUES(?,?,?)", (tid, seq, digest))
            self.db.executemany("INSERT INTO versions VALUES(?,?,?,?)", [
                (tid, seq, key, canonical(value)) for key, value in rows])
        return (tid, self.name, digest)

    def scan_versions(self):
        return [(tid, seq, key, json.loads(payload))
                for tid, seq, key, payload in self.db.execute(
                    "SELECT tid,seq,key,payload FROM versions ORDER BY seq,key")]

    def close(self):
        self.db.close()
