"""Simulator CLI.
  python run.py bootstrap --until 2017-06-01     replay all events up to a date as fast as possible
  python run.py live --speed 1440                continue from sim_state in real time (1 min = 1 day)
  python run.py pause | resume | status
"""
import argparse
import time
from contextlib import closing
from datetime import datetime
from typing import NamedTuple

import pandas as pd

import db
from clock import VirtualClock
from olist_data import EVENTS_PATH, TRANSACTION_TABLES
from replay import Replayer

CHUNK = 5000
EPOCH = datetime(1970, 1, 1)


class SimState(NamedTuple):
    virtual_ts: datetime
    speed: float
    status: str


def get_state(conn):
    """Current sim_state row (None before the first bootstrap). Commits so the next read sees fresh data."""
    with conn.cursor() as cur:
        cur.execute("SELECT virtual_ts, speed, status FROM sim_state WHERE id = 1")
        row = cur.fetchone()
    conn.commit()
    return SimState(*row) if row else None


def save_state(cur, ts, speed, status=None):
    """Upsert sim_state; status=None keeps the current status ('running' on first insert)."""
    cur.execute(
        "INSERT INTO sim_state (id, virtual_ts, speed, status) VALUES (1, %s, %s, COALESCE(%s,'running'))"
        " ON DUPLICATE KEY UPDATE virtual_ts = VALUES(virtual_ts), speed = VALUES(speed),"
        " status = COALESCE(%s, status)", (ts, speed, status, status))


def replay_until(conn, rp, events, start_ts, until, speed, status=None):
    """Apply events with start_ts < ts <= until in chunks; each chunk is one transaction incl. sim_state."""
    lo = events.ts.searchsorted(pd.Timestamp(start_ts), side="right")
    hi = events.ts.searchsorted(pd.Timestamp(until), side="right")
    with conn.cursor() as cur:
        for i in range(lo, hi, CHUNK):
            chunk = events.iloc[i:min(i + CHUNK, hi)]
            rp.apply(cur, chunk)
            last = until if i + CHUNK >= hi else chunk.ts.iloc[-1].to_pydatetime()
            save_state(cur, last, speed, status)
            conn.commit()
        if lo >= hi:  # nothing to apply, still move the virtual clock
            save_state(cur, until, speed, status)
            conn.commit()
    return max(hi - lo, 0)


def load():
    return Replayer(), pd.read_parquet(EVENTS_PATH)


def cmd_bootstrap(a):
    rp, events = load()
    with closing(db.connect()) as conn:
        st = get_state(conn)
        until = datetime.fromisoformat(a.until)
        t0 = time.time()
        n = replay_until(conn, rp, events, st.virtual_ts if st else EPOCH, until, a.speed, status="paused")
    print(f"bootstrap: applied {n:,} events up to {until} in {time.time() - t0:.1f}s (state=paused)")


def cmd_live(a):
    rp, events = load()
    last_event = events.ts.iloc[-1]
    with closing(db.connect()) as conn:
        st = get_state(conn)
        if not st:
            raise SystemExit("no sim_state: run `bootstrap` first")
        with conn.cursor() as cur:
            save_state(cur, st.virtual_ts, a.speed, "running")
        conn.commit()
        clock, paused, last_print, applied = VirtualClock(st.virtual_ts, a.speed), False, 0, 0
        print(f"live from {st.virtual_ts} at speed x{a.speed}")
        while True:
            st = get_state(conn)
            if st.status == "paused":
                paused = True
                time.sleep(1)
                continue
            if paused:  # re-anchor on resume so paused wall time is not replayed
                clock, paused = VirtualClock(st.virtual_ts, a.speed), False
            v_now = clock.now()
            applied += replay_until(conn, rp, events, st.virtual_ts, v_now, a.speed)
            if time.time() - last_print > 10:
                print(f"virtual={v_now:%Y-%m-%d %H:%M} applied_total={applied:,}", flush=True)
                last_print = time.time()
            if v_now > last_event:
                print("all events replayed; exiting")
                return
            time.sleep(1)


def cmd_set_status(status):
    def run(a):
        with closing(db.connect()) as conn, conn.cursor() as cur:
            cur.execute("UPDATE sim_state SET status = %s WHERE id = 1", (status,))
            conn.commit()
        print("sim_state.status =", status)
    return run


def cmd_status(a):
    with closing(db.connect()) as conn:
        print(get_state(conn))
        with conn.cursor() as cur:
            for t in TRANSACTION_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                print(f"  {t}: {cur.fetchone()[0]:,}")


def main():
    p = argparse.ArgumentParser()
    sp = p.add_subparsers(dest="cmd", required=True)
    b = sp.add_parser("bootstrap")
    b.add_argument("--until", required=True)
    b.add_argument("--speed", type=float, default=1440)
    b.set_defaults(fn=cmd_bootstrap)
    live = sp.add_parser("live")
    live.add_argument("--speed", type=float, default=1440)
    live.set_defaults(fn=cmd_live)
    sp.add_parser("pause").set_defaults(fn=cmd_set_status("paused"))
    sp.add_parser("resume").set_defaults(fn=cmd_set_status("running"))
    sp.add_parser("status").set_defaults(fn=cmd_status)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
