"""Regression checks for existing single-table behaviour and observation fixtures."""
import unittest
from warehouse import Warehouse
from replay import check


class BaselineChecks(unittest.TestCase):
    def setUp(self):
        self.app = Warehouse()

    def tearDown(self):
        self.app.close()

    def test_single_table_idempotence_and_conflict(self):
        self.app.declare("A", 1, ["samples"])
        receipt = self.app.land("A", "samples", [["x", 0], ["y", -2]])
        self.assertEqual(receipt, self.app.land("A", "samples", [["y", -2], ["x", 0]]))
        with self.assertRaises(ValueError):
            self.app.land("A", "samples", [["x", 1]])
        self.assertEqual(len(self.app.tables["samples"].scan_versions()), 2)
        self.assertEqual(self.app.catalog.receipts("A"), {"samples": receipt[2]})

    def test_atomic_bad_fragment_and_empty_receipt(self):
        self.app.declare("A", 1, ["samples", "qc"])
        with self.assertRaises(ValueError):
            self.app.land("A", "samples", [["x", 5], ["x", 6]])
        self.assertEqual(self.app.tables["samples"].scan_versions(), [])
        self.assertEqual(self.app.catalog.receipts("A"), {})
        self.app.land("A", "samples", [])
        self.assertIn("samples", self.app.catalog.receipts("A"))
        with self.assertRaises(ValueError):
            self.app.land("A", "qc", [["x", "unknown"]])

    def test_source_order_and_tombstone(self):
        self.app.declare("old", 7, ["samples", "qc"])
        self.app.declare("new", 8, ["samples", "qc"])
        self.app.land("new", "samples", [["x", None]])
        self.app.land("new", "qc", [["x", "bad"]])
        self.app.land("old", "samples", [["x", 70]])
        self.app.land("old", "qc", [["x", "ok"]])
        self.assertEqual(self.app.capture().read(),
                         {"samples": [], "qc": [["x", "bad"]], "total": 0})

    def test_declarations_and_two_table_query(self):
        self.app.declare("A", 1, ["samples", "qc"])
        self.app.declare("A", 1, ["qc", "samples"])
        with self.assertRaises(ValueError):
            self.app.declare("A", 1, ["samples"])
        with self.assertRaises(ValueError):
            self.app.land("missing", "samples", [])
        self.app.land("A", "samples", [["x", 10], ["y", 2]])
        self.app.land("A", "qc", [["x", "ok"], ["y", "bad"]])
        self.assertEqual(self.app.capture().read()["total"], 10)

    def test_recorded_observations(self):
        self.assertEqual(set(check()), {"partial", "overtake", "late_delete", "pending_delete"})


if __name__ == "__main__":
    unittest.main()
