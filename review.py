"""Report evidence checks. These checks do not prove a proposed protocol correct."""
import json
import re
import sys
import unittest
from pathlib import Path
import checks
from replay import cases, check

ROOT = Path(__file__).resolve().parent


def review():
    result = unittest.TextTestRunner(verbosity=1).run(
        unittest.defaultTestLoader.loadTestsFromModule(checks))
    if not result.wasSuccessful():
        raise AssertionError("baseline regressions failed")
    document = (ROOT / "DESIGN.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```visibility-evidence\s*\n(.*?)\n```", document, re.S)
    if len(blocks) != 1:
        raise AssertionError("one visibility-evidence block required")
    evidence = json.loads(blocks[0])
    observed = check()
    predicted = {name: case["target_reads"] for name, case in cases().items()}
    if evidence.get("observations") != observed:
        raise AssertionError("observations differ from replay")
    if evidence.get("predictions") != predicted:
        raise AssertionError("predictions differ from public anchors")
    references = re.findall(r"\b(landing|catalog|warehouse|query)\.py:([1-9]\d*)\b", document)
    if {name for name, _ in references} != {"landing", "catalog", "warehouse", "query"}:
        raise AssertionError("cite all four production modules using file.py:line")
    for name, line in references:
        if int(line) > len((ROOT / (name + ".py")).read_text(encoding="utf-8").splitlines()):
            raise AssertionError("source reference outside file")
    runtime = json.loads((ROOT / ".pointwise-runtime.json").read_text(encoding="utf-8"))
    if runtime["verify"] != ["python3", "review.py"]:
        raise AssertionError("verify must run report review")
    if runtime["demo"] != ["python3", "review.py", "--show"]:
        raise AssertionError("demo must display observed and predicted reads")
    return evidence


if __name__ == "__main__":
    if sys.argv[1:] not in ([], ["--show"]):
        raise SystemExit("usage: python3 review.py [--show]")
    evidence = review()
    if sys.argv[1:]:
        print(json.dumps(evidence, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        print("Report evidence and baseline regressions checked; protocol requires human review.")
