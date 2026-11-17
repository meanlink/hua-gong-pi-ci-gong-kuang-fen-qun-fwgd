"""协调端持久化：SQLite(WAL) + 进程锁，保证崩溃可恢复、提交可幂等。"""
from __future__ import annotations

import json
import sqlite3
import threading
import time

from .protocol import fingerprint


def now_ms() -> int:
    return int(time.time() * 1000)


class Store:
    def __init__(self, path: str):
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._schema()

    def _schema(self):
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS trials(
                  trial_id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  k INTEGER NOT NULL,
                  max_rounds INTEGER NOT NULL,
                  tol REAL NOT NULL,
                  status TEXT NOT NULL,
                  phase TEXT NOT NULL,
                  phase_round INTEGER NOT NULL DEFAULT 0,
                  station_urls TEXT NOT NULL,
                  config_fp TEXT NOT NULL,
                  std_params TEXT,
                  centers TEXT,
                  round_shifts TEXT NOT NULL DEFAULT '[]',
                  converged INTEGER NOT NULL DEFAULT 0,
                  stop_reason TEXT,
                  report TEXT,
                  created_at INTEGER NOT NULL,
                  updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS stations(
                  trial_id TEXT NOT NULL,
                  station_id TEXT NOT NULL,
                  base_url TEXT NOT NULL,
                  data_fp TEXT NOT NULL,
                  n_batches INTEGER NOT NULL,
                  registered_at INTEGER NOT NULL,
                  PRIMARY KEY(trial_id, station_id)
                );
                CREATE TABLE IF NOT EXISTS submissions(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  trial_id TEXT NOT NULL,
                  phase TEXT NOT NULL,
                  phase_round INTEGER NOT NULL,
                  station_id TEXT NOT NULL,
                  kind TEXT NOT NULL,
                  payload_fp TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  accepted INTEGER NOT NULL,
                  reject_reason TEXT,
                  created_at INTEGER NOT NULL,
                  UNIQUE(trial_id, phase, phase_round, station_id, kind)
                );
                CREATE TABLE IF NOT EXISTS audit(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  trial_id TEXT NOT NULL,
                  station_id TEXT,
                  event TEXT NOT NULL,
                  detail TEXT,
                  created_at INTEGER NOT NULL
                );
                """
            )
            self.conn.commit()

    # ---------------- 试验 ----------------
    def create_trial(self, trial_id, name, k, max_rounds, tol, station_urls):
        cfg = {"k": k, "max_rounds": max_rounds, "tol": tol,
               "station_urls": sorted(station_urls)}
        with self.lock:
            self.conn.execute(
                "INSERT INTO trials(trial_id,name,k,max_rounds,tol,status,phase,phase_round,"
                "station_urls,config_fp,round_shifts,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (trial_id, name, k, max_rounds, tol, "CREATED", "STANDARDIZE", 0,
                 json.dumps(cfg["station_urls"]), fingerprint(cfg), "[]", now_ms(), now_ms()))
            self.conn.commit()

    def get_trial(self, trial_id):
        with self.lock:
            row = self.conn.execute("SELECT * FROM trials WHERE trial_id=?", (trial_id,)).fetchone()
            return dict(row) if row else None

    def list_trials(self):
        with self.lock:
            rows = self.conn.execute("SELECT * FROM trials ORDER BY created_at DESC").fetchall()
            return [dict(r) for r in rows]

    def update_trial(self, trial_id, **fields):
        if not fields:
            return
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.lock:
            self.conn.execute(
                f"UPDATE trials SET {cols}, updated_at=? WHERE trial_id=?",
                (*fields.values(), now_ms(), trial_id))
            self.conn.commit()

    # ---------------- 站点 ----------------
    def register_station(self, trial_id, station_id, base_url, data_fp, n_batches):
        with self.lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO stations(trial_id,station_id,base_url,data_fp,n_batches,registered_at)"
                " VALUES(?,?,?,?,?,?)",
                (trial_id, station_id, base_url, data_fp, n_batches, now_ms()))
            self.conn.commit()

    def list_stations(self, trial_id):
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM stations WHERE trial_id=? ORDER BY station_id", (trial_id,)).fetchall()
            return [dict(r) for r in rows]

    # ---------------- 汇总提交（幂等） ----------------
    def insert_submission(self, trial_id, phase, phase_round, station_id, kind, payload,
                          accepted: bool, reject_reason=None):
        """唯一约束兜底：同站同轮同类重复提交不重复计入，返回(状态, 行)。"""
        fp = fingerprint(payload)
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        with self.lock:
            existing = self.conn.execute(
                "SELECT * FROM submissions WHERE trial_id=? AND phase=? AND phase_round=? "
                "AND station_id=? AND kind=?",
                (trial_id, phase, phase_round, station_id, kind)).fetchone()
            if existing:
                return "duplicate", dict(existing)
            self.conn.execute(
                "INSERT INTO submissions(trial_id,phase,phase_round,station_id,kind,payload_fp,"
                "payload,accepted,reject_reason,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (trial_id, phase, phase_round, station_id, kind, fp, body,
                 1 if accepted else 0, reject_reason, now_ms()))
            self.conn.commit()
            row = self.conn.execute(
                "SELECT * FROM submissions WHERE trial_id=? AND phase=? AND phase_round=? "
                "AND station_id=? AND kind=?",
                (trial_id, phase, phase_round, station_id, kind)).fetchone()
            return "accepted", dict(row)

    def accepted_submissions(self, trial_id, phase, phase_round, kind):
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM submissions WHERE trial_id=? AND phase=? AND phase_round=? AND kind=? "
                "AND accepted=1 ORDER BY station_id",
                (trial_id, phase, phase_round, kind)).fetchall()
            return [dict(r) for r in rows]

    def station_phase_status(self, trial_id):
        """每站在各阶段/轮次的最新受理情况，供页面展示缺席（不算零贡献）。"""
        with self.lock:
            rows = self.conn.execute(
                "SELECT phase,phase_round,station_id,accepted,reject_reason FROM submissions "
                "WHERE trial_id=? ORDER BY id", (trial_id,)).fetchall()
            return [dict(r) for r in rows]

    # ---------------- 审计 ----------------
    def audit(self, trial_id, event, station_id=None, detail=None):
        with self.lock:
            self.conn.execute(
                "INSERT INTO audit(trial_id,station_id,event,detail,created_at) VALUES(?,?,?,?,?)",
                (trial_id, station_id, event,
                 json.dumps(detail, ensure_ascii=False) if detail is not None else None, now_ms()))
            self.conn.commit()

    def list_audit(self, trial_id, limit=200):
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM audit WHERE trial_id=? ORDER BY id DESC LIMIT ?", (trial_id, limit)).fetchall()
            return [dict(r) for r in rows]
