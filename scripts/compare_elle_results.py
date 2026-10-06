#!/usr/bin/env python3
"""Compare adya against Elle's own expected results.

elle-cli ships histories with the JSON verdicts Elle produced for them
(test/test-data/*-result.json). This script runs adya on each one with the
same options and compares the verdict, the anomaly types, and the weakest
models ruled out.

    python scripts/compare_elle_results.py <elle-cli>/test/test-data [adya-binary]
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

# Per-case options, as in elle-cli's test suite (elle_cli_test.clj).
OPTS = {
    "list-append-gh-30": {"models": ["serializable"]},
    "rw-register-keys-wfr-valid": {"flags": ["--wfr-keys"]},
    "rw-register-keys-sequential-valid": {"flags": ["--sequential-keys"]},
    "rw-register-keys-sequential-anomaly": {"flags": ["--sequential-keys"]},
    "rw-register-keys-linearizable-valid": {"flags": ["--linearizable-keys"]},
    "rw-register-keys-linearizable-anomaly": {"flags": ["--linearizable-keys"]},
    "rw-register-partial-info": {"flags": ["--linearizable-keys"]},
    "rw-register-transaction-order-valid": {"transaction_order": {"1": 0, "3": 1, "5": 2}},
    "rw-register-process-anomaly": {"models": ["strong-session-serializable"]},
    "rw-register-model-serializable": {"models": ["serializable"]},
    "rw-register-model-snapshot-isolation": {"models": ["snapshot-isolation"]},
    "rw-register-model-strong-snapshot": {"models": ["strong-snapshot-isolation"]},
    "list-append-model-read-uncommitted": {"models": ["read-uncommitted"]},
    "list-append-model-snapshot-isolation": {"models": ["snapshot-isolation"]},
    "list-append-model-read-committed": {"models": ["read-committed"]},
}
# Version-order inference adya does not implement (yet).
UNSUPPORTED = {}
# Elle prints strict serializability under its alias.
ALIAS = {"strong-serializable": "strict-serializable"}


def norm(models):
    return sorted({ALIAS.get(m, m) for m in models})


def main():
    data = Path(sys.argv[1])
    adya = sys.argv[2] if len(sys.argv) > 2 else "target/release/adya"
    same = differ = skipped = 0
    for result in sorted(data.glob("*-result.json")):
        case = result.name[: -len("-result.json")]
        history = data / f"{case}.json"
        if not history.exists():
            history = data / f"{case}.edn"
        workload = "rw-register" if case.startswith("rw-register") else "list-append"
        if not case.startswith(("list-append", "rw-register")) or not history.exists():
            continue
        if case in UNSUPPORTED:
            print(f"skip  {case}: needs {UNSUPPORTED[case]}")
            skipped += 1
            continue
        o = OPTS.get(case, {})
        cmd = [adya, "check", "--json", "-m", workload, "-c", ",".join(o.get("models", ["strict-serializable"])), "-a", "G0"]
        cmd += o.get("flags", [])
        if "transaction_order" in o:
            order = Path(tempfile.mkdtemp()) / "order.json"
            order.write_text(json.dumps(o["transaction_order"]))
            cmd += ["--transaction-order", str(order)]
        out = subprocess.run(cmd + [str(history)], capture_output=True, text=True)
        try:
            got = json.loads(out.stdout)
        except json.JSONDecodeError:
            print(f"ERROR {case}: {out.stderr.strip()}")
            differ += 1
            continue
        want = json.loads(result.read_text())
        w = (want.get("valid?"), sorted(want.get("anomaly-types", [])), norm(want.get("not", [])))
        g = (got["valid"], sorted(got["anomaly_types"]), norm(got["not"]))
        if w == g:
            same += 1
            print(f"same  {case}: {g[0]} {g[1]}")
        else:
            differ += 1
            print(f"DIFF  {case}:\n   elle {w}\n   adya {g}")
    print(f"\n{same} same, {differ} different, {skipped} skipped")
    sys.exit(1 if differ else 0)


if __name__ == "__main__":
    main()
