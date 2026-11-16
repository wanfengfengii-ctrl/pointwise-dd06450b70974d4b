"""Source declarations and landing acknowledgements; not a visibility index."""
import json
import sqlite3
from landing import TABLES


class Catalog:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript("""
            CREATE TABLE declarations(tid TEXT PRIMARY KEY, seq INTEGER UNIQUE, tables TEXT);
            CREATE TABLE receipts(tid TEXT, table_name TEXT, digest TEXT,
                                  PRIMARY KEY(tid,table_name));
        """)

    def declare(self, tid, seq, tables):
        if not isinstance(tid, str) or not tid or type(seq) is not int or not 1 <= seq <= 2**63 - 1:
            raise ValueError("invalid declaration")
        if (not isinstance(tables, list) or not tables
                or any(t not in TABLES for t in tables) or len(set(tables)) != len(tables)):
            raise ValueError("invalid table set")
        encoded = json.dumps(sorted(tables))
        old = self.db.execute(
            "SELECT seq,tables FROM declarations WHERE tid=?", (tid,)).fetchone()
        if old is not None:
            if old != (seq, encoded):
                raise ValueError("declaration conflict")
            return
        last = self.db.execute("SELECT COALESCE(MAX(seq),0) FROM declarations").fetchone()[0]
        if seq <= last:
            raise ValueError("source sequence must increase")
        with self.db:
            self.db.execute("INSERT INTO declarations VALUES(?,?,?)", (tid, seq, encoded))

    def declaration(self, tid):
        row = self.db.execute(
            "SELECT seq,tables FROM declarations WHERE tid=?", (tid,)).fetchone()
        if row is None:
            raise ValueError("unknown transaction")
        return row[0], json.loads(row[1])

    def record(self, receipt):
        tid, table, digest = receipt
        _, tables = self.declaration(tid)
        if table not in tables:
            raise ValueError("undeclared table")
        old = self.db.execute(
            "SELECT digest FROM receipts WHERE tid=? AND table_name=?", (tid, table)).fetchone()
        if old is not None and old[0] != digest:
            raise ValueError("receipt conflict")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO receipts VALUES(?,?,?)", receipt)

    def receipts(self, tid):
        self.declaration(tid)
        return dict(self.db.execute(
            "SELECT table_name,digest FROM receipts WHERE tid=?", (tid,)))

    def close(self):
        self.db.close()
