"""Current query path: latest landed source version, independently per table."""


class Handle:
    def __init__(self, tables):
        self.tables = tables

    def read(self):
        visible = {}
        for name, table in self.tables.items():
            winners = {}
            for tid, seq, key, value in table.scan_versions():
                if key not in winners or seq > winners[key][0]:
                    winners[key] = (seq, value)
            visible[name] = {key: value for key, (seq, value) in winners.items()
                             if value is not None}
        samples, qc = visible["samples"], visible["qc"]
        return {
            "samples": sorted([key, value] for key, value in samples.items()),
            "qc": sorted([key, value] for key, value in qc.items()),
            "total": sum(value for key, value in samples.items() if qc.get(key) == "ok"),
        }


class Query:
    def __init__(self, tables):
        self.tables = tables

    def capture(self):
        # Baseline captures references, not an immutable logical view.
        return Handle(self.tables)
