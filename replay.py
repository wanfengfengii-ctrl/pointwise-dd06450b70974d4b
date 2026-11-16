"""Deterministic observations of the unmodified baseline."""
import json
import sys
from pathlib import Path
from warehouse import Warehouse


def cases():
    return json.loads(Path(__file__).with_name("scenarios.json").read_text(encoding="utf-8"))


def run(case):
    app = Warehouse()
    handles, reads = {}, []
    try:
        for declaration in case["declarations"]:
            app.declare(*declaration)
        for step, event in enumerate(case["events"], 1):
            op, *args = event
            if op == "land":
                app.land(*args)
            elif op == "capture":
                handles[args[0]] = app.capture()
            elif op == "read":
                reads.append({"step": step, "handle": args[0],
                              "result": handles[args[0]].read()})
            else:
                raise ValueError("unknown event")
        return reads
    finally:
        app.close()


def observe():
    return {name: run(case) for name, case in cases().items()}


def check():
    actual = observe()
    expected = {name: case["baseline_reads"] for name, case in cases().items()}
    if actual != expected:
        raise AssertionError({"actual": actual, "expected": expected})
    return actual


if __name__ == "__main__":
    if sys.argv[1:] not in ([], ["--check"]):
        raise SystemExit("usage: python3 replay.py [--check]")
    print(json.dumps(check(), ensure_ascii=False, sort_keys=True, indent=2))
