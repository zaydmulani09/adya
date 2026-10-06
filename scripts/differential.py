#!/usr/bin/env python3
"""Differential test: adya vs Jepsen's Elle (via elle-cli) on random histories.

Generates histories with adya's simulated database across isolation levels,
workloads and seeds, checks each with both tools under several consistency
models, and compares verdicts, anomaly types and the weakest models ruled out.

When Elle's cycle search times out its answer is partial, so the case is
re-checked by both tools with a 60 s budget. If Elle still cannot finish (or
does not return at all within ELLE_WALL_CLOCK), the case is counted as
inconclusive rather than as a difference.

    python scripts/differential.py ADYA_BIN ELLE_CLI_JAR [count]
"""
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ISOLATIONS = ["serializable", "snapshot-isolation", "read-committed", "read-uncommitted", "lost-update"]
MODELS = ["strict-serializable", "serializable", "snapshot-isolation", "repeatable-read", "read-committed"]
ALIAS = {"strong-serializable": "strict-serializable"}
ELLE_WALL_CLOCK = 300


class ElleCrashed(Exception):
    pass


def norm(models):
    return sorted({ALIAS.get(m, m) for m in models})


def elle(jar, workload, model, path, search_ms):
    out = subprocess.run(
        ["java", "-Xmx6g", "-jar", jar, "-m", workload, "-c", model, "-s", str(search_ms), "-v", "json", str(path)],
        capture_output=True, text=True, timeout=ELLE_WALL_CLOCK, stdin=subprocess.DEVNULL,
    )
    text = out.stdout
    start = text.find("{")
    if start < 0:
        first = next((l for l in out.stderr.splitlines() if l.strip()), "no output")
        raise ElleCrashed(first)
    return json.loads(text[start:])


def adya(binary, workload, model, path, search_ms):
    out = subprocess.run(
        [binary, "check", "--json", "-m", workload, "-c", model, "-a", "G0",
         "--cycle-search-timeout", str(search_ms), str(path)],
        capture_output=True, text=True,
    )
    return json.loads(out.stdout)


def summary(r, valid_key, types_key):
    return (r.get(valid_key), sorted(r.get(types_key, [])), norm(r.get("not", [])))


def main():
    binary, jar = sys.argv[1], sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    tmp = Path("out/differential")
    tmp.mkdir(parents=True, exist_ok=True)
    same = differ = inconclusive = 0
    elle_s = adya_s = 0.0
    for n in range(count):
        iso = ISOLATIONS[n % len(ISOLATIONS)]
        workload = "rw-register" if n % 4 == 3 else "list-append"
        model = MODELS[(n // len(ISOLATIONS)) % len(MODELS)]
        path = tmp / f"h{n}.json"
        subprocess.run(
            [binary, "run", "sim", "-i", iso, "-m", workload, "-n", "300", "-p", "5", "--keys", "5",
             "--seed", str(n), "-o", str(path)],
            capture_output=True,
        )
        # elle-cli wants a JSON array; adya reads either form.
        ops = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        path.write_text(json.dumps(ops))
        label = f"#{n} {workload} sim={iso} -c {model}"

        verdicts = None
        for search_ms in (1000, 60000):
            try:
                t = time.time()
                e = elle(jar, workload, model, path, search_ms)
                elle_s += time.time() - t
            except ElleCrashed as err:
                elle_s += time.time() - t
                verdicts = None
                print(f"????  {label}: elle-cli crashed: {err} ({path})")
                break
            except subprocess.TimeoutExpired:
                elle_s += ELLE_WALL_CLOCK
                verdicts = None
                print(f"????  {label}: elle-cli did not finish in {ELLE_WALL_CLOCK}s ({path})")
                break
            t = time.time()
            a = adya(binary, workload, model, path, search_ms)
            adya_s += time.time() - t
            verdicts = (summary(e, "valid?", "anomaly-types"), summary(a, "valid", "anomaly_types"))
            if "cycle-search-timeout" not in verdicts[0][1]:
                break
        if verdicts is None or "cycle-search-timeout" in verdicts[0][1]:
            inconclusive += 1
            if verdicts:
                print(f"????  {label}: Elle's cycle search timed out\n   elle {verdicts[0]}\n   adya {verdicts[1]}")
            continue
        want, got = verdicts
        if want == got:
            same += 1
            print(f"same  {label}: {got[0]} {got[1]}")
        else:
            differ += 1
            print(f"DIFF  {label}  ({path})\n   elle {want}\n   adya {got}")
    print(f"\n{same} same, {differ} different, {inconclusive} inconclusive")
    print(f"total check time: elle-cli {elle_s:.1f}s (including JVM startup), adya {adya_s:.1f}s")
    sys.exit(1 if differ else 0)


if __name__ == "__main__":
    main()
