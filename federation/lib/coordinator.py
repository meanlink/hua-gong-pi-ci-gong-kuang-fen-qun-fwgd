"""联合试验协调端。

核心协议保证（对应需求逐条落地）：
1. 启动后固定参与站、数据指纹与分群数 K：start 时快照并写 config_fp，之后任何变更拒绝；
2. 同轮汇总齐备才推进：屏障按固定站点集合计数，缺席站不补零、不视同零贡献；
3. 站点重试不重复计入：(试验,阶段,轮次,站点,类型) 唯一约束 + 幂等返回；
4. 断线/协调端中断后恢复原轮：全部状态落 SQLite(WAL)，重启从磁盘读出当前轮；
5. 旧轮迟到回包拒收：phase_round 与当前轮不一致即 rejected 并审计；
6. 并发推进不跳轮：全局锁内完成校验-入库-屏障检查-阶段推进的原子序列；
7. 收敛后各站归群与全局中心全部就绪才发布：FINAL 屏障 + 发布闸门，半成品只读不可批准。
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .protocol import (FEATURES, FEATURE_LABELS, PHASE_LABELS, TRIAL_ABORTED,
                       TRIAL_CREATED, TRIAL_PUBLISHED, TRIAL_RUNNING,
                       aggregate_std, fingerprint, inverse_centers,
                       kmeans_pp_init, merge_iteration)
from .store import Store, now_ms
from .web import http_json, request_json, send_json


class Coordinator:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.store = Store(db_path)
        self.lock = threading.RLock()
        # 仅进程内自检使用的屏障冻结开关，HTTP 接口不暴露；默认关闭即生产行为
        self.freeze_advance = False

    # ----------------------------- 试验管理 -----------------------------
    def create_trial(self, body):
        name = str(body.get("name") or "未命名联合试验").strip()[:80]
        try:
            k = int(body["k"])
            max_rounds = int(body.get("max_rounds", 20))
            tol = float(body.get("tol", 1e-4))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("K 须为整数，收敛阈值与最大轮次须为数值。") from exc
        if not 2 <= k <= 6:
            raise ValueError("分群数 K 须为 2~6 的整数。")
        if not 1 <= max_rounds <= 100 or tol <= 0:
            raise ValueError("最大轮次须 1~100，收敛阈值须为正数。")
        urls = body.get("station_urls") or []
        if not isinstance(urls, list) or not urls:
            raise ValueError("须至少指定一个参与站点 URL。")
        if len(set(urls)) != len(urls):
            raise ValueError("参与站点 URL 不可重复。")
        trial_id = "FED-" + secrets.token_hex(4).upper()
        self.store.create_trial(trial_id, name, k, max_rounds, tol, urls)
        self.store.audit(trial_id, "trial_created", detail={"name": name, "k": k, "stations": urls})
        return trial_id

    def register_station(self, body):
        trial = self._require(body.get("trial_id"))
        with self.lock:
            if trial["status"] != TRIAL_CREATED:
                # 已启动后允许同指纹补登记（站点重启），但数据/站点集合不可变
                existing = next((s for s in self.store.list_stations(trial["trial_id"])
                                 if s["station_id"] == body["station_id"]), None)
                if not existing or existing["data_fp"] != body["data_fp"]:
                    raise PermissionError("试验已启动，参与站与数据已固定，拒绝变更。")
                return {"registered": True, "frozen": True}
            urls = json.loads(trial["station_urls"])
            if body["base_url"] not in urls:
                raise PermissionError("该站点不在试验固定参与站名单内。")
            existing = self.store.list_stations(trial["trial_id"])
            same_id = next((s for s in existing if s["station_id"] == body["station_id"]), None)
            if same_id:
                if same_id["data_fp"] != body["data_fp"]:
                    raise PermissionError("同站号数据指纹与已登记快照不一致，拒绝变更。")
                return {"registered": True, "frozen": False}
            if any(s["base_url"] == body["base_url"] for s in existing):
                raise PermissionError("该站点 URL 已由固定名单中的其他站号登记，拒绝重复占用。")
            self.store.register_station(trial["trial_id"], body["station_id"], body["base_url"],
                                        body["data_fp"], int(body["n_batches"]))
            self.store.audit(trial["trial_id"], "station_registered", body["station_id"],
                             {"data_fp": body["data_fp"], "n_batches": body["n_batches"]})
            return {"registered": True, "frozen": False}

    def start_trial(self, trial_id):
        trial = self._require(trial_id)
        with self.lock:
            if trial["status"] != TRIAL_CREATED:
                raise PermissionError("试验已启动，参与站、数据与分群数不可再变更。")
            stations = self.store.list_stations(trial_id)
            urls = set(json.loads(trial["station_urls"]))
            registered_urls = {s["base_url"] for s in stations}
            missing = sorted(urls - registered_urls)
            if missing:
                raise PermissionError(f"固定参与站未全部登记，缺失: {missing}")
            fps = {s["data_fp"] for s in stations}
            if len(fps) != len(stations):
                raise PermissionError("参与站数据指纹重复，疑似同源数据，拒绝启动。")
            self.store.update_trial(trial_id, status=TRIAL_RUNNING, phase="STANDARDIZE", phase_round=0)
            self.store.audit(trial_id, "trial_started",
                             detail={"stations": [s["station_id"] for s in stations],
                                     "k": trial["k"], "config_fp": trial["config_fp"],
                                     "data_fps": sorted(fps)})
            return self.trial_view(trial_id)

    # ----------------------------- 汇总提交与屏障 -----------------------------
    def submit(self, envelope):
        trial_id = envelope.get("trial_id")
        trial = self._require(trial_id)
        phase = envelope.get("phase")
        phase_round = envelope.get("phase_round")
        station_id = envelope.get("station_id")
        kind = envelope.get("kind")
        payload = envelope.get("payload")
        with self.lock:
            stations = self.store.list_stations(trial_id)
            station = next((s for s in stations if s["station_id"] == station_id), None)
            # —— 固定参与站校验 ——
            if not station:
                return self._reject(trial, envelope, "站点不属于本试验固定参与站集合。")
            if envelope.get("data_fp") != station["data_fp"]:
                return self._reject(trial, envelope, "数据指纹与启动快照不一致，拒绝计入。")
            if trial["status"] != TRIAL_RUNNING:
                return self._reject(trial, envelope, f"试验状态为 {trial['status']}，不再受理汇总。")
            # —— 旧轮迟到回包拒收 ——
            if phase != trial["phase"] or phase_round != trial["phase_round"]:
                reason = (f"迟到回包拒收：当前 {trial['phase']}#轮{trial['phase_round']}，"
                          f"回包属于 {phase}#轮{phase_round}。")
                self.store.insert_submission(trial_id, phase, phase_round, station_id, kind,
                                             payload, accepted=False, reject_reason=reason)
                self.store.audit(trial_id, "stale_rejected", station_id,
                                 {"got": f"{phase}#{phase_round}",
                                  "current": f"{trial['phase']}#{trial['phase_round']}"})
                return {"accepted": False, "status": "rejected_stale", "reason": reason}
            # —— 幂等入库（重试不重复计入） ——
            status, row = self.store.insert_submission(
                trial_id, phase, phase_round, station_id, kind, payload, accepted=True)
            if status == "duplicate":
                self.store.audit(trial_id, "duplicate_ignored", station_id,
                                 {"phase": phase, "round": phase_round, "kind": kind})
                return {"accepted": True, "status": "duplicate_ignored",
                        "reason": "同站同轮同类汇总已计入，重试不重复计入。",
                        "payload_fp": row["payload_fp"]}
            self.store.audit(trial_id, "submission_accepted", station_id,
                             {"phase": phase, "round": phase_round, "kind": kind, "fp": row["payload_fp"]})
            # —— 屏障：固定站点同轮齐备才推进（缺席不补零） ——
            if not self.freeze_advance:
                self._advance_if_barrier(trial_id)
            return {"accepted": True, "status": "accepted", "payload_fp": row["payload_fp"]}

    def _reject(self, trial, envelope, reason):
        self.store.insert_submission(
            trial["trial_id"], envelope.get("phase", "?"), envelope.get("phase_round", -1),
            envelope.get("station_id", "?"), envelope.get("kind", "?"),
            envelope.get("payload", {}), accepted=False, reject_reason=reason)
        self.store.audit(trial["trial_id"], "submission_rejected",
                         envelope.get("station_id"), {"reason": reason})
        return {"accepted": False, "status": "rejected", "reason": reason}

    def _advance_if_barrier(self, trial_id):
        trial = self.store.get_trial(trial_id)
        stations = self.store.list_stations(trial_id)
        expected = len(stations)
        phase, rnd = trial["phase"], trial["phase_round"]
        kind = self._kind_for(phase)
        rows = self.store.accepted_submissions(trial_id, phase, rnd, kind)
        got = len(rows)
        self.store.audit(trial_id, "barrier_check",
                         detail={"phase": phase, "round": rnd, "got": got, "expected": expected})
        if got < expected:
            return  # 缺席站保持等待，绝不以零贡献顶替
        payloads = [json.loads(r["payload"]) for r in rows]
        if phase == "STANDARDIZE":
            std = aggregate_std(payloads)
            self.store.update_trial(trial_id, std_params=json.dumps(std),
                                    phase="INIT", phase_round=0)
            self.store.audit(trial_id, "phase_advanced", detail={"to": "INIT", "std_n": std["n"]})
        elif phase == "INIT":
            # 合并各站候选中心（按簇计数加权），协调端做确定性 k-means++ 得到全局初始中心
            weighted = []
            for p in payloads:
                for c, w in zip(p["centers"], p["counts"]):
                    weighted.append((c, max(w, 1)))
            import random
            centers = kmeans_pp_init(weighted, trial["k"], random.Random(42))
            self.store.update_trial(trial_id, centers=json.dumps(centers),
                                    phase="ITERATE", phase_round=1)
            self.store.audit(trial_id, "phase_advanced", detail={"to": "ITERATE", "round": 1})
        elif phase == "ITERATE":
            centers, global_counts = merge_iteration(payloads, trial["k"])
            prev = json.loads(trial["centers"])
            shift = max(sum((a - b) ** 2 for a, b in zip(n, o)) ** 0.5
                        for n, o in zip(centers, prev))
            shifts = json.loads(trial["round_shifts"]) + [round(shift, 8)]
            converged = shift <= trial["tol"]
            stop_reason = None
            if converged:
                stop_reason = f"最大中心位移 {shift:.2e} ≤ 阈值 {trial['tol']:.0e}，收敛"
            elif rnd >= trial["max_rounds"]:
                stop_reason = f"达到最大轮次 {trial['max_rounds']}，停止迭代"
            self.store.update_trial(
                trial_id, centers=json.dumps(centers), round_shifts=json.dumps(shifts),
                converged=1 if converged else 0, stop_reason=stop_reason)
            self.store.audit(trial_id, "iteration_merged",
                             detail={"round": rnd, "shift": shifts[-1], "counts": global_counts})
            if stop_reason:
                self.store.update_trial(trial_id, phase="FINAL", phase_round=0)
                self.store.audit(trial_id, "phase_advanced", detail={"to": "FINAL", "reason": stop_reason})
            else:
                self.store.update_trial(trial_id, phase_round=rnd + 1)  # 只在锁内推进，绝不跳轮
        elif phase == "FINAL":
            report = self._build_report(trial_id, payloads, stations)
            self.store.update_trial(trial_id, phase="DONE", report=json.dumps(report, ensure_ascii=False))
            self.store.audit(trial_id, "final_ready", detail={"report_fp": report["report_fp"]})

    @staticmethod
    def _kind_for(phase):
        return {"STANDARDIZE": "std", "INIT": "init_centers",
                "ITERATE": "iteration", "FINAL": "final"}[phase]

    # ----------------------------- 发布闸门 -----------------------------
    def publish(self, trial_id):
        trial = self._require(trial_id)
        with self.lock:
            if trial["status"] == TRIAL_PUBLISHED:
                return self.trial_view(trial_id)
            if trial["phase"] != "DONE" or not trial["report"]:
                raise PermissionError("各站归群与全局中心尚未全部就绪，半成品不供批准或简报。")
            report = json.loads(trial["report"])
            stations = self.store.list_stations(trial_id)
            if len(report["stations"]) != len(stations):
                raise PermissionError("归群站点数与固定参与站不一致，拒绝发布。")
            report["published_at"] = now_ms()
            report["status"] = TRIAL_PUBLISHED
            self.store.update_trial(trial_id, status=TRIAL_PUBLISHED,
                                    report=json.dumps(report, ensure_ascii=False))
            self.store.audit(trial_id, "trial_published", detail={"report_fp": report["report_fp"]})
            return self.trial_view(trial_id)

    def abort(self, trial_id, reason):
        trial = self._require(trial_id)
        with self.lock:
            if trial["status"] in (TRIAL_PUBLISHED,):
                raise PermissionError("已发布试验不可终止。")
            self.store.update_trial(trial_id, status=TRIAL_ABORTED, stop_reason=reason)
            self.store.audit(trial_id, "trial_aborted", detail={"reason": reason})
            return self.trial_view(trial_id)

    # ----------------------------- 报告与视图 -----------------------------
    def _build_report(self, trial_id, final_payloads, stations):
        trial = self.store.get_trial(trial_id)
        centers_z = json.loads(trial["centers"])
        std = json.loads(trial["std_params"])
        centers_raw = inverse_centers(centers_z, std)
        order = sorted(range(trial["k"]), key=lambda c: centers_raw[c][0])  # 按温度排序显示标签
        label_map = {internal: display + 1 for display, internal in enumerate(order)}
        cluster_table = []
        total_n = sum(p["n"] for p in final_payloads)
        for internal in order:
            count = sum(p["per_cluster"][str(internal)]["count"] for p in final_payloads)
            row = {"cluster": f"工况族群{label_map[internal]}", "count": count,
                   "share_pct": round(count / total_n * 100, 1)}
            row.update({f: round(v, 2) for f, v in zip(FEATURES, centers_raw[internal])})
            cluster_table.append(row)
        station_views = []
        for s, p in zip(stations, final_payloads):
            # 终稿校验：归群计数须与该站批次数一致，且每批次可追溯
            assigned = sum(c["count"] for c in p["per_cluster"].values())
            batches = [b for c in p["per_cluster"].values() for b in c["batches"]]
            if assigned != s["n_batches"] or len(set(batches)) != s["n_batches"]:
                raise ValueError(f"站点 {s['station_id']} 归群摘要与批次台账不一致，拒绝定稿。")
            station_views.append({
                "station_id": s["station_id"], "data_fp": s["data_fp"], "n_batches": s["n_batches"],
                "silhouette": p["silhouette"],
                "clusters": {f"工况族群{label_map[int(c)]}": v["count"] for c, v in p["per_cluster"].items()},
                "batch_trace": {f"工况族群{label_map[int(c)]}": v["batches"]
                                for c, v in p["per_cluster"].items()},
            })
        global_silhouette = round(sum(p["silhouette"] * p["n"] for p in final_payloads) / total_n, 6)
        report = {
            "trial_id": trial_id, "k": trial["k"], "total_batches": total_n,
            "global_silhouette": global_silhouette,
            "std_params": {"mean": [round(v, 4) for v in std["mean"]],
                           "std": [round(v, 4) for v in std["std"]]},
            "cluster_table": cluster_table, "stations": station_views,
            "round_shifts": json.loads(trial["round_shifts"]),
            "stop_reason": trial["stop_reason"],
            "feature_labels": FEATURE_LABELS,
            "report_fp": fingerprint({"trial_id": trial_id, "centers": centers_z,
                                      "fps": sorted(s["data_fp"] for s in stations)}),
            "built_at": now_ms(), "status": "ready",
        }
        return report

    def trial_view(self, trial_id):
        trial = self._require(trial_id)
        stations = self.store.list_stations(trial_id)
        expected = len(stations)
        submissions = self.store.station_phase_status(trial_id)
        # 每站在当前轮的受理状态；未交即 absent（缺席，不算零贡献）
        current_kind = self._kind_for(trial["phase"]) if trial["phase"] in ("STANDARDIZE", "INIT", "ITERATE", "FINAL") else None
        station_views = []
        for s in stations:
            if trial["phase"] == "DONE":
                state = "finalized"  # 归群摘要已随 FINAL 屏障齐备并定稿
            else:
                state = "absent"
                for sub in submissions:
                    if sub["station_id"] != s["station_id"]:
                        continue
                    if sub["phase"] == trial["phase"] and sub["phase_round"] == trial["phase_round"]:
                        state = "accepted" if sub["accepted"] else "rejected"
            station_views.append({**s, "current_round_state": state})
        view = {k: (json.loads(v) if k in ("station_urls", "std_params", "centers",
                                           "round_shifts", "report") and v else v)
                for k, v in trial.items()}
        view["phase_label"] = PHASE_LABELS[trial["phase"]]
        view["stations"] = station_views
        view["expected_stations"] = expected
        view["barrier_got"] = expected if trial["phase"] == "DONE" else \
            sum(1 for s in station_views if s["current_round_state"] == "accepted")
        view["publish_ready"] = trial["phase"] == "DONE" and bool(trial["report"])
        view["audit_tail"] = self.store.list_audit(trial_id, limit=15)
        return view

    def assignments_for(self, station_id):
        """站点轮询：返回该站参与且运行中的试验及当前轮上下文。"""
        result = []
        for trial in self.store.list_trials():
            if trial["status"] != TRIAL_RUNNING:
                continue
            if any(s["station_id"] == station_id for s in self.store.list_stations(trial["trial_id"])):
                result.append(self.trial_view(trial["trial_id"]))
        return {"trials": result}

    def recover(self, trial_id=None):
        """协调端恢复：从 SQLite 重建视图，确认停留原轮；可选驱动站点重试。"""
        trials = self.store.list_trials()
        out = []
        for t in trials:
            if trial_id and t["trial_id"] != trial_id:
                continue
            view = self.trial_view(t["trial_id"])
            recovered = {"trial_id": t["trial_id"], "status": t["status"],
                         "phase": t["phase"], "phase_round": t["phase_round"],
                         "barrier": f"{view['barrier_got']}/{view['expected_stations']}"}
            if t["status"] == TRIAL_RUNNING:
                for s in view["stations"]:
                    if s["current_round_state"] == "absent":
                        try:
                            http_json("POST", s["base_url"] + "/api/station/retry",
                                      {"trial_id": t["trial_id"]}, timeout=5)
                            recovered.setdefault("retry_signaled", []).append(s["station_id"])
                        except Exception as exc:
                            recovered.setdefault("unreachable", []).append(f"{s['station_id']}: {exc}")
            out.append(recovered)
            self.store.audit(t["trial_id"], "coordinator_recovered",
                             detail={"phase": t["phase"], "round": t["phase_round"]})
        return {"recovered": out}

    def recover_on_startup(self):
        """进程重启后恢复：对每个 RUNNING 试验在锁内重评当前轮屏障。

        已受理的汇总不会重发（站点侧亦有幂等状态），重评只在屏障齐备时继续推进，
        因此恢复后仍停留原轮、不跳轮、不重复计入。
        """
        for t in self.store.list_trials():
            if t["status"] == TRIAL_RUNNING:
                with self.lock:
                    self._advance_if_barrier(t["trial_id"])
                self.store.audit(t["trial_id"], "startup_recovery",
                                 detail={"phase": t["phase"], "round": t["phase_round"]})

    def _require(self, trial_id):
        trial = self.store.get_trial(trial_id)
        if not trial:
            raise KeyError(f"试验 {trial_id} 不存在。")
        return trial


def make_handler(coord: Coordinator, static_dir: str):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _error(self, code, message):
            send_json(self, code, {"detail": message})

        def do_GET(self):
            path = self.path.split("?")[0]
            try:
                if path == "/health":
                    return send_json(self, 200, {"status": "ok"})
                if path == "/api/trials":
                    return send_json(self, 200, {"trials": [coord.trial_view(t["trial_id"])
                                                            for t in coord.store.list_trials()]})
                if path.startswith("/api/trials/audit/"):
                    trial_id = path.rsplit("/", 1)[-1]
                    return send_json(self, 200, {"audit": coord.store.list_audit(trial_id, 500)})
                if path.startswith("/api/trials/"):
                    trial_id = path.rsplit("/", 1)[-1]
                    return send_json(self, 200, coord.trial_view(trial_id))
                if path.startswith("/api/station/assignments"):
                    from urllib.parse import parse_qs, urlparse
                    qs = parse_qs(urlparse(self.path).query)
                    return send_json(self, 200, coord.assignments_for(qs.get("station_id", [""])[0]))
                return self._serve_static(path, static_dir)
            except KeyError as exc:
                return self._error(404, str(exc).strip("'"))
            except Exception as exc:
                traceback.print_exc()
                return self._error(500, str(exc))

        def do_POST(self):
            path = self.path.split("?")[0]
            try:
                body = request_json(self)
                if path == "/api/trials":
                    trial_id = coord.create_trial(body)
                    return send_json(self, 201, coord.trial_view(trial_id))
                if path == "/api/station/register":
                    return send_json(self, 200, coord.register_station(body))
                if path == "/api/station/submissions":
                    return send_json(self, 200, coord.submit(body))
                if path.startswith("/api/trials/start/"):
                    return send_json(self, 200, coord.start_trial(path.rsplit("/", 1)[-1]))
                if path.startswith("/api/trials/publish/"):
                    return send_json(self, 200, coord.publish(path.rsplit("/", 1)[-1]))
                if path.startswith("/api/trials/abort/"):
                    return send_json(self, 200, coord.abort(path.rsplit("/", 1)[-1], body.get("reason", "手工终止")))
                if path == "/api/recover":
                    return send_json(self, 200, coord.recover(body.get("trial_id")))
                # 页面故障控制代理：协调端转发到站点，浏览器无需直连各站
                if path.startswith("/api/proxy/station/"):
                    return self._proxy_station(body)
                return self._error(404, "接口不存在")
            except (ValueError, PermissionError) as exc:
                return self._error(400, str(exc))
            except KeyError as exc:
                return self._error(404, str(exc).strip("'"))
            except Exception as exc:
                traceback.print_exc()
                return self._error(500, str(exc))

        def _proxy_station(self, body):
            station_url = body.pop("station_url")
            action = body.pop("action")
            mapping = {"/api/station/fault": "fault", "/api/station/retry": "retry",
                       "/api/station/inject-stale": "stale"}
            target = next(k for k, v in mapping.items() if v == action)
            return send_json(self, 200, http_json("POST", station_url + target, body))

        def _serve_static(self, path, static_dir):
            if path in ("/", ""):
                path = "/index.html"
            safe = os.path.normpath(path).lstrip("/")
            full = os.path.join(static_dir, safe)
            if not os.path.abspath(full).startswith(os.path.abspath(static_dir)) or not os.path.isfile(full):
                return self._error(404, "静态资源不存在")
            ctype = "text/html; charset=utf-8" if full.endswith(".html") else \
                "text/css; charset=utf-8" if full.endswith(".css") else \
                "application/javascript; charset=utf-8" if full.endswith(".js") else "application/octet-stream"
            with open(full, "rb") as fh:
                data = fh.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    return Handler


def main():
    parser = argparse.ArgumentParser(description="联邦联合试验-协调端")
    parser.add_argument("--port", type=int, default=8060)
    parser.add_argument("--db", default="./federation-data/coordinator.db")
    parser.add_argument("--static", default=os.path.join(os.path.dirname(__file__), "..", "webui"))
    args = parser.parse_args()
    coord = Coordinator(args.db)
    coord.recover_on_startup()
    server = ThreadingHTTPServer(("127.0.0.1", args.port),
                                 make_handler(coord, os.path.abspath(args.static)))
    print(f"[coordinator] listening on http://127.0.0.1:{args.port} (db={args.db})")
    server.serve_forever()


if __name__ == "__main__":
    main()
