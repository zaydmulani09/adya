#!/usr/bin/env python3
"""Differential test: adya vs Jepsen's Elle (via elle-cli) on random histories.

Generates histories with adya's simulated database across isolation levels,
workloads and seeds, checks each with both tools under several consistency
models, and compares verdicts, anomaly types and the weakest models ruled out.

    python scripts/differential.py ADYA_BIN ELLE_CLI_JAR [count]
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ISOLATIONS = ["serializable", "snapshot-isolation", "read-committed", "read-uncommitted", "lost-update"]
MODELS = ["strict-serializable", "serializable", "snapshot-isolation", "repeatable-read", "read-committed"]
ALIAS = {"strong-serializable": "strict-serializable"}


def norm(models):
    return sorted({ALIAS.get(m, m) for m in models})


def elle(jar, workload, model, path):
    out = subprocess.run(
        ["java", "-jar", jar, "-m", workload, "-c", model, "-v", "json", str(path)],
        capture_output=True, text=True, timeout=600, stdin=subprocess.DEVNULL,
    )
    # elle-cli prints the JSON result after the file name.
    text = out.stdout
    start = text.find("{")
    if start < 0:
        raise RuntimeError(f"elle-cli produced no JSON: {out.stdout!r} {out.stderr[-2000:]!r}")
    return json.loads(text[start:])


def adya(binary, workload, model, path):
    out = subprocess.run([binary, "check", "--json", "-m", workload, "-c", model, "-a", "G0", str(path)], capture_output=True, text=True)
    return json.loads(out.stdout)


def main():
    binary, jar = sys.argv[1], sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    tmp = Path(tempfile.mkdtemp())
    same = differ = 0
    for n in range(count):
        iso = ISOLATIONS[n % len(ISOLATIONS)]
        workload = "rw-register" if n % 4 == 3 else "list-append"
        model = MODELS[(n // len(ISOLATIONS)) % len(MODELS)]
        path = tmp / f"h{n}.jsonl"
        subprocess.run(
            [binary, "run", "sim", "-i", iso, "-m", workload, "-n", "300", "-p", "5", "--keys", "5",
             "--seed", str(n), "-o", str(path)],
            capture_output=True,
        )
        e = elle(jar, workload, model, path)
        a = adya(binary, workload, model, path)
        want = (e.get("valid?"), sorted(e.get("anomaly-types", [])), norm(e.get("not", [])))
        got = (a["valid"], sorted(a["anomaly_types"]), norm(a["not"]))
        label = f"#{n} {workload} sim={iso} -c {model}"
        if want == got:
            same += 1
            print(f"same  {label}: {got[0]} {got[1]}")
        else:
            differ += 1
            print(f"DIFF  {label}  ({path})\n   elle {want}\n   adya {got}")
    print(f"\n{same} same, {differ} different")
    sys.exit(1 if differ else 0)


if __name__ == "__main__":
    main()
