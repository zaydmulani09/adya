#!/usr/bin/env python3
"""An `adya run exec` client: runs each transaction against SQLite.

adya starts one copy of this script per client process and sends it one JSON
line per transaction; it replies with one line saying how the transaction
ended. Swap the SQL for your own database to test it from Python.

    adya run exec --url "python3 examples/exec_sqlite.py test.db" -c strict-serializable
    adya run exec -m rw-register --url "python3 examples/exec_sqlite.py test.db rw-register"
"""
import json
import sqlite3
import sys

registers = len(sys.argv) > 2 and sys.argv[2] == "rw-register"
db = sqlite3.connect(sys.argv[1] if len(sys.argv) > 1 else "exec.db", timeout=5, isolation_level=None)
db.execute("PRAGMA journal_mode=WAL")
db.execute("CREATE TABLE IF NOT EXISTS lists (k INTEGER PRIMARY KEY, v TEXT NOT NULL)")
db.execute("CREATE TABLE IF NOT EXISTS regs (k INTEGER PRIMARY KEY, v INTEGER NOT NULL)")


def run(txn):
    out = []
    for f, k, v in txn:
        if f == "append":
            db.execute("INSERT INTO lists (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = v || ',' || excluded.v", (k, str(v)))
            out.append([f, k, v])
        elif f == "w":
            db.execute("INSERT INTO regs (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v", (k, v))
            out.append([f, k, v])
        elif registers:
            row = db.execute("SELECT v FROM regs WHERE k = ?", (k,)).fetchone()
            out.append([f, k, row[0] if row else None])
        else:
            row = db.execute("SELECT v FROM lists WHERE k = ?", (k,)).fetchone()
            out.append([f, k, [int(x) for x in row[0].split(",")] if row else []])
    return out


for line in sys.stdin:
    txn = json.loads(line)["value"]
    try:
        db.execute("BEGIN IMMEDIATE")
        value = run(txn)
    except sqlite3.Error as e:
        if db.in_transaction:
            db.execute("ROLLBACK")
        print(json.dumps({"type": "fail", "error": str(e)}), flush=True)
        continue
    try:
        db.execute("COMMIT")
        print(json.dumps({"type": "ok", "value": value}), flush=True)
    except sqlite3.Error as e:
        # A failed COMMIT that leaves the transaction open did not commit.
        if db.in_transaction:
            db.execute("ROLLBACK")
            print(json.dumps({"type": "fail", "error": str(e)}), flush=True)
        else:
            print(json.dumps({"type": "info", "error": str(e)}), flush=True)
