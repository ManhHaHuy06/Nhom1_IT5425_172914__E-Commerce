"""Simulator CLI.
  python run.py bootstrap --until 2017-06-01     replay all events up to a date as fast as possible
  python run.py live --speed 1440                continue from sim_state in real time (1 min = 1 day)
  python run.py pause | resume | status
"""
import argparse
import time
from datetime import datetime

import pandas as pd

import db
from clock import VirtualClock
from replay import Replayer

EVENTS = "/app/events.parquet"
CHUNK = 5000


def get_state(cur):
    cur.execute("SELECT virtual_ts, speed, status FROM sim_state WHERE id = 1")
    return cur.fetchone()


def save_state(cur, ts, speed, status=None):
    cur.execute(
        "INSERT INTO sim_state (id, virtual_ts, speed, status) VALUES (1, %s, %s, COALESCE(%s,'running'))"
        " ON DUPLICATE KEY UPDATE virtual_ts = VALUES(virtual_ts), speed = VALUES(speed),"
        " status = COALESCE(%s, status)", (ts, speed, status, status))


def replay_until(conn, rp, events, start_ts, until, speed, status=None):
    """Apply events with start_ts < ts <= until in chunks; each chunk is one transaction incl. sim_state."""
    lo = events.ts.searchsorted(pd.Timestamp(start_ts), side="right")
    hi = events.ts.searchsorted(pd.Timestamp(until), side="right")
    cur = conn.cursor()
    n = 0
    for i in range(lo, hi, CHUNK):
        chunk = events.iloc[i:min(i + CHUNK, hi)]
        rp.apply(cur, chunk)
        last = until if i + CHUNK >= hi else chunk.ts.iloc[-1].to_pydatetime()
        save_state(cur, last, speed, status)
        conn.commit()
        n += len(chunk)
    if lo >= hi:  # nothing to apply, still move the virtual clock
        save_state(cur, until, speed, status)
        conn.commit()
    return n


def cmd_bootstrap(a):
    conn, rp, events = db.connect(), Replayer(), pd.read_parquet(EVENTS)
    cur = conn.cursor()
    st = get_state(cur)
    start = st[0] if st else datetime(1970, 1, 1)
    until = datetime.fromisoformat(a.until)
    t0 = time.time()
    n = replay_until(conn, rp, events, start, until, a.speed, status="paused")
    print(f"bootstrap: applied {n:,} events up to {until} in {time.time() - t0:.1f}s (state=paused)")


def cmd_live(a):
    conn, rp, events = db.connect(), Replayer(), pd.read_parquet(EVENTS)
    cur = conn.cursor()
    st = get_state(cur)
    if not st:
        raise SystemExit("no sim_state: run `bootstrap` first")
    save_state(cur, st[0], a.speed, "running")
    conn.commit()
    clock, last_print, applied = VirtualClock(st[0], a.speed), 0, 0
    print(f"live from {st[0]} at speed x{a.speed}")
    while True:
        cur.execute("SELECT status FROM sim_state WHERE id = 1")
        conn.commit()
        if cur.fetchone()[0] == "paused":
            time.sleep(1)
            st = get_state(cur)
            clock = VirtualClock(st[0], a.speed)  # re-anchor so paused time is not replayed
            continue
        v_now = clock.now()
        cur_state = get_state(cur)[0]
        conn.commit()
        applied += replay_until(conn, rp, events, cur_state, v_now, a.speed)
        if time.time() - last_print > 10:
            print(f"virtual={v_now:%Y-%m-%d %H:%M} applied_total={applied:,}", flush=True)
            last_print = time.time()
        if v_now > events.ts.iloc[-1]:
            print("all events replayed; exiting")
            return
        time.sleep(1)


def cmd_set_status(status):
    def f(a):
        conn = db.connect()
        cur = conn.cursor()
        cur.execute("UPDATE sim_state SET status = %s WHERE id = 1", (status,))
        conn.commit()
        print("sim_state.status =", status)
    return f


def cmd_status(a):
    conn = db.connect()
    cur = conn.cursor()
    print(get_state(cur))
    for t in ["customers", "orders", "order_items", "order_payments", "order_reviews"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  {t}: {cur.fetchone()[0]:,}")


def main():
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("bootstrap")
    b.add_argument("--until", required=True)
    b.add_argument("--speed", type=float, default=1440)
    b.set_defaults(fn=cmd_bootstrap)
    l = sp.add_parser("live")
    l.add_argument("--speed", type=float, default=1440)
    l.set_defaults(fn=cmd_live)
    sp.add_parser("pause").set_defaults(fn=cmd_set_status("paused"))
    sp.add_parser("resume").set_defaults(fn=cmd_set_status("running"))
    sp.add_parser("status").set_defaults(fn=cmd_status)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
