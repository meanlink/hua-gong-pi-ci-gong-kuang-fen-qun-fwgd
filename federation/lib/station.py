"""本地站点服务（各厂一站，独立进程）。

职责边界：
- 原始批次记录只存在本站 SQLite 与内存中，任何上报仅含统计汇总/批次标签摘要；
- 轮询协调端获取当前轮次，按轮提交标准化、初始中心、迭代加权和、定稿归群摘要；
- 站点重试不重复计入（本地去重 + 协调端唯一约束双保险）；
- 故障注入：offline（断线）、duplicate（重复回包）、stale（旧轮迟到）、delay（延迟）；
- 进程被杀后重启，依据本地持久化恢复原轮，不重发已确认的汇总。
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .protocol import (FEATURES, RANDOM_SEED, data_fingerprint, fingerprint,
                       kmeans, local_iteration_summary, local_std_summary,
                       silhouette, standardize, validate_records)
from .web import http_json, request_json

FAULT_MODES = ("none", "offline", "duplicate", "stale", "delay")


# ----------------------------- 本地模拟数据（仅站内可见） -----------------------------

def generate_local_records(station_id: str, batches: int):
    """生成本站批次：三个物理运行中心 + 站点偏移，站点间分布不同但族群同源。"""
    seed = int(hashlib_sha(station_id)[:8], 16)
    rng = random.Random(1010 + seed % 1000)
    base = [[78, 2.2, 46, 89], [96, 3.6, 30, 94], [112, 5.1, 22, 83]]
    scales = [2.2, 0.18, 2.4, 1.6]
    site_bias = [(seed % 7) - 3, ((seed % 5) - 2) * 0.1, ((seed % 9) - 4), ((seed % 5) - 2) * 0.8]
    mix = [0.45, 0.35, 0.2]
    mix = [w + ((seed >> c) % 3 - 1) * 0.05 for c, w in enumerate(mix)]
    total = sum(mix)
    mix = [w / total for w in mix]
    labels = [0] * round(mix[0] * batches) + [1] * round(mix[1] * batches)
    labels += [2] * (batches - len(labels))
    rng.shuffle(labels)
    records = []
    prefix = station_id.upper().replace("STATION-", "S")
    for i, lab in enumerate(labels):
        center = [base[lab][j] + site_bias[j] for j in range(4)]
        values = [rng.gauss(center[j], scales[j]) for j in range(4)]
        values[3] = min(100.0, max(0.0, values[3]))
        records.append({"batch_id": f"{prefix}-B{i+1:03}",
                        **{f: round(float(v), 2) for f, v in zip(FEATURES, values)}})
    return records


def hashlib_sha(text):
    import hashlib
    return hashlib.sha256(text.encode()).hexdigest()


# ----------------------------- 站点本地存储 -----------------------------

class StationStore:
    def __init__(self, path):
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS records(batch_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS state(
              trial_id TEXT, phase TEXT, phase_round INTEGER, kind TEXT,
              payload_fp TEXT, status TEXT, detail TEXT, updated_at INTEGER,
              PRIMARY KEY(trial_id, phase, phase_round, kind));
            CREATE TABLE IF NOT EXISTS meta(meta_key TEXT PRIMARY KEY, meta_value TEXT);
            """)
        self.conn.commit()

    def save_records(self, records):
        with self.lock:
            self.conn.execute("DELETE FROM records")
            self.conn.executemany("INSERT INTO records(batch_id,payload) VALUES(?,?)",
                                  [(r["batch_id"], json.dumps(r, ensure_ascii=False)) for r in records])
            self.conn.commit()

    def load_records(self):
        with self.lock:
            return [json.loads(r["payload"]) for r in self.conn.execute("SELECT payload FROM records ORDER BY batch_id")]

    def mark_state(self, trial_id, phase, phase_round, kind, fp, status, detail=None):
        with self.lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO state(trial_id,phase,phase_round,kind,payload_fp,status,detail,updated_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (trial_id, phase, phase_round, kind, fp, status, detail, int(time.time() * 1000)))
            self.conn.commit()

    def get_state(self, trial_id, phase, phase_round, kind):
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM state WHERE trial_id=? AND phase=? AND phase_round=? AND kind=?",
                (trial_id, phase, phase_round, kind)).fetchone()
            return dict(row) if row else None

    def all_state(self, trial_id=None):
        with self.lock:
            sql = "SELECT * FROM state" + (" WHERE trial_id=?" if trial_id else "")
            rows = self.conn.execute(sql, ([trial_id] if trial_id else [])).fetchall()
            return [dict(r) for r in rows]

    def set_fault(self, mode, note=""):
        with self.lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO meta(meta_key,meta_value) VALUES('fault',?)",
                (json.dumps({"mode": mode, "note": note}, ensure_ascii=False),))
            self.conn.commit()

    def get_fault(self):
        with self.lock:
            row = self.conn.execute("SELECT meta_value FROM meta WHERE meta_key='fault'").fetchone()
            return json.loads(row["meta_value"]) if row else {"mode": "none", "note": ""}


# ----------------------------- 站点主体 -----------------------------

class Station:
    def __init__(self, station_id, coordinator_url, data_dir, batches=60, base_url=""):
        self.station_id = station_id
        self.coordinator_url = coordinator_url.rstrip("/")
        self._base_url = base_url.rstrip("/")
        os.makedirs(data_dir, exist_ok=True)
        self.store = StationStore(os.path.join(data_dir, f"{station_id}.db"))
        records = self.store.load_records()
        if not records:
            records = generate_local_records(station_id, batches)
            self.store.save_records(records)
        self.records = records
        self.matrix = validate_records(records, k=6)  # 用最大 K 校验，任何试验均可参加
        self.data_fp = data_fingerprint(records)
        self.z_cache = {}          # trial_id -> 标准化后的本地矩阵
        self.center_cache = {}     # trial_id -> 最近一次广播中心
        self.logs = []
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)

    def log(self, message):
        line = {"ts": int(time.time() * 1000), "message": message}
        self.logs.append(line)
        self.logs[:] = self.logs[-100:]

    # ---------- 与协调端通信（带故障注入） ----------
    def _coord_fault(self):
        """仅作用于协调端方向的出站；本地控制面（故障/重试）保持可达，模拟带外管理口。"""
        return self.store.get_fault()["mode"]

    def post(self, path, payload):
        fault = self._coord_fault()
        if fault == "offline":
            raise ConnectionError("站点处于模拟断线状态，拒绝到协调端的出站连接")
        if fault == "delay":
            time.sleep(4.0)
        return http_json("POST", f"{self.coordinator_url}{path}", payload)

    def get(self, path):
        if self._coord_fault() == "offline":
            raise ConnectionError("站点处于模拟断线状态，拒绝到协调端的出站连接")
        return http_json("GET", f"{self.coordinator_url}{path}")

    def submit(self, trial_id, phase, phase_round, kind, payload, force_stale_round=None):
        """带幂等与故障注入的提交。force_stale_round 用于模拟旧轮迟到回包。"""
        send_round = force_stale_round if force_stale_round is not None else phase_round
        fp = fingerprint(payload)
        local = self.store.get_state(trial_id, phase, phase_round, kind)
        if local and local["status"] == "accepted" and force_stale_round is None:
            return {"skipped": "already_accepted", "payload_fp": fp}
        envelope = {"trial_id": trial_id, "station_id": self.station_id,
                    "phase": phase, "phase_round": send_round, "kind": kind,
                    "data_fp": self.data_fp, "payload": payload}
        try:
            resp = self.post("/api/station/submissions", envelope)
        except Exception as exc:  # 留待下轮 poll 重试，不丢状态
            self.store.mark_state(trial_id, phase, phase_round, kind, fp, "pending", str(exc))
            self.log(f"提交失败待重试 {phase}#{phase_round}: {exc}")
            return {"error": str(exc)}
        if resp.get("accepted"):
            self.store.mark_state(trial_id, phase, phase_round, kind, fp, "accepted")
            self.log(f"汇总已受理 {phase}#{phase_round} ({kind}) fp={fp}")
        else:
            self.store.mark_state(trial_id, phase, phase_round, kind, fp, "rejected", resp.get("reason"))
            self.log(f"汇总被拒收 {phase}#{send_round}: {resp.get('reason')}")
        # 模拟重复回包：协调端必须幂等，不重复计入
        if self.store.get_fault()["mode"] == "duplicate" and force_stale_round is None and resp.get("accepted"):
            dup = self.post("/api/station/submissions", envelope)
            self.log(f"模拟重复回包，协调端判定: {dup.get('status')}")
            resp["duplicate_echo"] = dup
        return resp

    # ---------- 各阶段本地计算 ----------
    @staticmethod
    def _as_obj(value):
        """协调端视图可能已解析 JSON，也可能仍是字符串，两种形态都兼容。"""
        return json.loads(value) if isinstance(value, str) else value

    def _z(self, trial, trial_id):
        if trial_id not in self.z_cache:
            self.z_cache[trial_id] = standardize(self.matrix, self._as_obj(trial["std_params"]))
        return self.z_cache[trial_id]

    def act(self, trial):
        """每个轮询周期尝试补齐当前轮：已受理即跳过，未受理（含断线后的 pending）必重发。

        协调端唯一约束兜底，因此重发天然幂等，不会重复计入。
        """
        trial_id = trial["trial_id"]
        phase, rnd = trial["phase"], trial["phase_round"]
        k = trial["k"]
        if phase == "STANDARDIZE":
            self.submit(trial_id, phase, 0, "std", local_std_summary(self.matrix))
        elif phase == "INIT":
            z = self._z(trial, trial_id)
            # 站点本地 k-means++ 候选中心 + 惯性，仅交中心与簇计数（局部汇总）
            centers, labels, inertia, _ = kmeans(z, k, seed=RANDOM_SEED)
            counts = [labels.count(c) for c in range(k)]
            self.submit(trial_id, phase, 0, "init_centers",
                        {"centers": [[round(v, 8) for v in c] for c in centers],
                         "counts": counts, "inertia": round(inertia, 8)})
        elif phase == "ITERATE":
            z = self._z(trial, trial_id)
            centers = self._as_obj(trial["centers"])
            self.center_cache[trial_id] = centers
            summary = local_iteration_summary(z, centers)
            self.submit(trial_id, phase, rnd, "iteration",
                        {"counts": summary["counts"], "vec_sum": summary["vec_sum"]})
        elif phase == "FINAL":
            z = self._z(trial, trial_id)
            centers = self._as_obj(trial["centers"])
            from .protocol import assign
            labels, _ = assign(z, centers)
            score = silhouette(z, labels, k)
            # 归群定稿：每批次只交批次号+簇标签（标签摘要，不含原始工况值），按簇汇总计数
            per_cluster = {str(c): {"count": 0, "batches": []} for c in range(k)}
            for batch, lab in zip(self.records, labels):
                per_cluster[str(lab)]["count"] += 1
                per_cluster[str(lab)]["batches"].append(batch["batch_id"])
            payload = {"n": len(self.records), "silhouette": round(score, 6),
                       "per_cluster": per_cluster}
            self.submit(trial_id, phase, 0, "final", payload)

    def _poll_loop(self):
        while not self._stop.wait(2.0):
            try:
                assignments = self.get("/api/station/assignments?station_id=" + self.station_id)
            except Exception as exc:
                self.log(f"轮询协调端失败（将重试，恢复原轮）: {exc}")
                continue
            running_ids = {t["trial_id"] for t in assignments.get("trials", [])}
            # 自动登记：发现 CREATED 试验且本站 URL 在固定名单内即登记（数据指纹随包上报）
            try:
                self._auto_register(running_ids)
            except Exception as exc:
                self.log(f"自动登记失败（下轮重试）: {exc}")
            for trial in assignments.get("trials", []):
                if trial["status"] != "RUNNING":
                    continue
                try:
                    self.act(trial)
                except Exception as exc:
                    self.log(f"处理 {trial['trial_id']} {trial['phase']} 出错: {exc}")

    def _auto_register(self, running_ids):
        trials = http_json("GET", f"{self.coordinator_url}/api/trials")["trials"]
        for trial in trials:
            if trial["trial_id"] in running_ids or trial["status"] != "CREATED":
                continue
            if self._my_port_url() not in (trial.get("station_urls") or []):
                continue
            resp = self.post("/api/station/register", {
                "trial_id": trial["trial_id"], "station_id": self.station_id,
                "base_url": self._my_port_url(), "data_fp": self.data_fp,
                "n_batches": len(self.records)})
            if resp.get("registered"):
                self.log(f"已登记试验 {trial['trial_id']}（数据指纹 {self.data_fp}）")

    def _my_port_url(self):
        if not getattr(self, "_base_url", None):
            # 由启动参数显式传入最可靠；未传时从协调端可达地址无法反推，故由 main 注入
            raise RuntimeError("站点自身 URL 未配置")
        return self._base_url

    # ---------- 手工控制（页面/演示脚本用） ----------
    def inject_stale(self, trial_id):
        """模拟旧轮迟到回包：取上一轮编号重发，协调端应拒收。"""
        trial = http_json("GET", f"{self.coordinator_url}/api/trials/{trial_id}")
        if trial["phase"] != "ITERATE" or trial["phase_round"] < 1:
            return {"ok": False, "reason": "当前无历史迭代轮可制造迟到回包"}
        old_round = trial["phase_round"] - 1
        z = self._z(trial, trial_id)
        # 旧轮中心已不可得时使用当前中心构造，重点验证轮次拒收逻辑
        payload = local_iteration_summary(z, self._as_obj(trial["centers"]))
        resp = self.submit(trial_id, "ITERATE", old_round, "iteration",
                           {"counts": payload["counts"], "vec_sum": payload["vec_sum"]},
                           force_stale_round=old_round)
        return {"ok": True, "stale_round": old_round, "response": resp}

    def retry_pending(self, trial_id=None):
        """站点重试：对所有 pending 汇总重新投递；已受理项不重发（不重复计入）。"""
        retried = []
        for row in self.store.all_state(trial_id):
            if row["status"] == "pending":
                trial = http_json("GET", f"{self.coordinator_url}/api/trials/{row['trial_id']}")
                self.act(trial)
                retried.append(f"{row['phase']}#{row['phase_round']}/{row['kind']}")
        return {"retried": retried}

    def status(self):
        return {"station_id": self.station_id, "coordinator": self.coordinator_url,
                "data_fp": self.data_fp, "n_batches": len(self.records),
                "fault": self.store.get_fault(), "pending": [r for r in self.store.all_state() if r["status"] == "pending"],
                "logs": list(reversed(self.logs[:20]))}


def make_handler(station: Station):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            station.log(args[0] % args[1:])

        def _send(self, code, body):
            data = json.dumps(body, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path.startswith("/health"):
                return self._send(200, {"status": "ok", "station_id": station.station_id,
                                        "fault": station.store.get_fault()})
            if self.path.startswith("/api/station/status"):
                return self._send(200, station.status())
            return self._send(404, {"detail": "not found"})

        def do_POST(self):
            body = request_json(self)
            try:
                if self.path == "/api/station/fault":
                    mode = body.get("mode", "none")
                    if mode not in FAULT_MODES:
                        return self._send(400, {"detail": f"故障模式须为 {FAULT_MODES}"})
                    station.store.set_fault(mode, body.get("note", ""))
                    station.log(f"故障注入: {mode} {body.get('note','')}")
                    return self._send(200, {"ok": True, "fault": station.store.get_fault()})
                if self.path == "/api/station/retry":
                    return self._send(200, station.retry_pending(body.get("trial_id")))
                if self.path == "/api/station/inject-stale":
                    return self._send(200, station.inject_stale(body["trial_id"]))
            except Exception as exc:
                return self._send(500, {"detail": str(exc)})
            return self._send(404, {"detail": "not found"})

    return Handler


def main():
    parser = argparse.ArgumentParser(description="联邦联合试验-本地站点")
    parser.add_argument("--station-id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--coordinator", default="http://127.0.0.1:8060")
    parser.add_argument("--data-dir", default="./federation-data/stations")
    parser.add_argument("--batches", type=int, default=60)
    parser.add_argument("--base-url", default="")
    args = parser.parse_args()
    base_url = args.base_url or f"http://127.0.0.1:{args.port}"
    station = Station(args.station_id, args.coordinator, args.data_dir, args.batches, base_url)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(station))
    station.thread.start()
    station.log(f"站点 {args.station_id} 启动于端口 {args.port}，数据指纹 {station.data_fp}")
    print(f"[{args.station_id}] listening on http://127.0.0.1:{args.port} (fp={station.data_fp}, n={len(station.records)})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        station._stop.set()
        server.shutdown()


if __name__ == "__main__":
    main()
