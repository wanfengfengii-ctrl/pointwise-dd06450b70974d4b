"""Public application entry. Landing commits before receipt recording."""
from catalog import Catalog
from landing import TABLES, Table
from query import Query


class Warehouse:
    def __init__(self):
        self.catalog = Catalog()
        self.tables = {name: Table(name) for name in TABLES}
        self.query = Query(self.tables)

    def declare(self, tid, seq, tables):
        self.catalog.declare(tid, seq, tables)

    def land(self, tid, table, rows):
        seq, tables = self.catalog.declaration(tid)
        if table not in tables:
            raise ValueError("undeclared table")
        receipt = self.tables[table].land(tid, seq, rows)
        self.catalog.record(receipt)
        return receipt

    def capture(self):
        return self.query.capture()

    def close(self):
        for table in self.tables.values():
            table.close()
        self.catalog.close()
