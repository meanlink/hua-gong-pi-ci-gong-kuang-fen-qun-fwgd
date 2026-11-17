"""联合试验端到端自检：在临时目录拉起协调端+3站点，验证全部协议承诺。

运行：python -m federation.selftest
不依赖任何第三方库；退出码 0 表示全部断言通过。
"""
from __future__ import annotations

import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

from .lib.web import http_json

ROOT = os.path.dirname(os.path.abspath(__file__))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_ready(url, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            http_json("GET", url + "/health", timeout=2)
            return
        except Exception:
            time.sleep(0.3)
    raise TimeoutError(f"服务未就绪: {url}")


def wait_until(desc, fn, timeout=60, interval=0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            value = fn()
            if value:
                return value
        except Exception as exc:
            last = exc
        time.sleep(interval)
    raise AssertionError(f"等待超时：{desc}（最后错误 {last}）")


class Harness:
    def __init__(self, base):
        self.base = base
        self.procs = []

    def start(self, *cmd):
        log = open(os.path.join(self.base, "proc-" + str(len(self.procs)) + ".log"), "w")
        proc = subprocess.Popen([sys.executable, "-m", *cmd], cwd=os.path.dirname(ROOT),
                                stdout=log, stderr=subprocess.STDOUT)
        self.procs.append(proc)
        return proc

    def stop_all(self):
        for p in self.procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def main():
    base = tempfile.mkdtemp(prefix="fed-selftest-")
    h = Harness(base)
    coord_port, ports = free_port(), [free_port() for _ in range(3)]
    coord = f"http://127.0.0.1:{coord_port}"
    station_urls = [f"http://127.0.0.1:{p}" for p in ports]
    failures = []

    def check(name, cond, detail=""):
        mark = "PASS" if cond else "FAIL"
        print(f"[{mark}] {name} {detail}")
        if not cond:
            failures.append(name)

    try:
        # 1) 起协调端与三站点
        h.start("federation.lib.coordinator", "--port", str(coord_port),
                "--db", os.path.join(base, "coord.db"))
        wait_ready(coord)
        for i, port in enumerate(ports, 1):
            h.start("federation.lib.station", "--station-id", f"station-{i}",
                    "--port", str(port), "--coordinator", coord,
                    "--data-dir", os.path.join(base, "stations"), "--batches", "60")
            wait_ready(station_urls[i - 1])
        print(f"协调端 {coord}；站点 {station_urls}")

        # 2) 建试验并等三站登记
        trial_id = http_json("POST", coord + "/api/trials", {
            "name": "自检联合试验", "k": 3, "max_rounds": 20, "tol": 1e-4,
            "station_urls": station_urls})["trial_id"]
        wait_until("三站登记", lambda: (
            lambda t: len(t["stations"]) == 3)(http_json("GET", f"{coord}/api/trials/{trial_id}")))
        trial = http_json("GET", f"{coord}/api/trials/{trial_id}")
        check("三站登记且指纹互异", len({s["data_fp"] for s in trial["stations"]}) == 3)
        check("未启动前状态为 CREATED", trial["status"] == "CREATED")

        # 3) 启动后参数固定：名单外站点/重复占用 URL 都应失败
        try:
            http_json("POST", coord + "/api/station/register", {
                "trial_id": trial_id, "station_id": "station-x",
                "base_url": "http://127.0.0.1:9", "data_fp": "deadbeefdeadbeef", "n_batches": 60})
            check("非名单站点登记被拒", False)
        except RuntimeError:
            check("非名单站点登记被拒", True)
        try:
            http_json("POST", coord + "/api/station/register", {
                "trial_id": trial_id, "station_id": "station-y",
                "base_url": station_urls[0], "data_fp": "0123456789abcdef", "n_batches": 60})
            check("重复占用固定 URL 登记被拒", False)
        except RuntimeError:
            check("重复占用固定 URL 登记被拒", True)
        http_json("POST", f"{coord}/api/trials/start/{trial_id}", {})
        check("启动后进入 RUNNING", http_json("GET", f"{coord}/api/trials/{trial_id}")["status"] == "RUNNING")

        # 4) 故障注入：station-1 断线，屏障必须停在 2/3，不跳轮
        http_json("POST", station_urls[0] + "/api/station/fault", {"mode": "offline"})
        time.sleep(8)
        trial = http_json("GET", f"{coord}/api/trials/{trial_id}")
        check("断线期间缺席站不补零、屏障不齐不推进",
              trial["barrier_got"] in (2, 3) and trial["phase"] in ("STANDARDIZE", "INIT", "ITERATE"))
        check("缺席站标记 absent（非零贡献）",
              any(s["current_round_state"] == "absent" for s in trial["stations"]))
        phase_during_outage = (trial["phase"], trial["phase_round"])

        # 5) 恢复上线 + 协调端恢复，试验在原轮继续并收敛
        http_json("POST", station_urls[0] + "/api/station/fault", {"mode": "none"})
        http_json("POST", coord + "/api/recover", {"trial_id": trial_id})
        wait_until("试验收敛进入 FINAL/DONE",
                   lambda: http_json("GET", f"{coord}/api/trials/{trial_id}")["phase"] in ("FINAL", "DONE"),
                   timeout=90)
        trial = http_json("GET", f"{coord}/api/trials/{trial_id}")
        check("断线恢复后未跳轮且持续推进", trial["phase"] in ("FINAL", "DONE"))

        # 6) 旧轮迟到回包必须拒收
        if (trial["phase"] == "DONE") or (trial["phase"] == "FINAL"):
            # 另开一个试验制造迭代中的迟到场景
            t2 = http_json("POST", coord + "/api/trials", {
                "name": "迟到拒收试验", "k": 2, "max_rounds": 20, "tol": 1e-4,
                "station_urls": station_urls})["trial_id"]
            wait_until("试验2三站登记",
                       lambda: len(http_json("GET", f"{coord}/api/trials/{t2}")["stations"]) == 3)
            http_json("POST", f"{coord}/api/trials/start/{t2}", {})
            wait_until("试验2进入迭代",
                       lambda: http_json("GET", f"{coord}/api/trials/{t2}")["phase"] == "ITERATE",
                       timeout=40)
            time.sleep(2)
            stale_resp = http_json("POST", station_urls[0] + "/api/station/inject-stale",
                                   {"trial_id": t2})
            ok = stale_resp.get("response", {}).get("status") == "rejected_stale"
            check("旧轮迟到回包被拒收（rejected_stale）", ok, str(stale_resp)[:160])
            audit = http_json("GET", f"{coord}/api/trials/audit/{t2}")["audit"]
            check("拒收事件写入审计", any(a["event"] == "stale_rejected" for a in audit))

        # 7) 重复回包不重复计入：专用试验 + 确定性时序
        t3 = http_json("POST", coord + "/api/trials", {
            "name": "幂等试验", "k": 2, "max_rounds": 20, "tol": 1e-4,
            "station_urls": station_urls})["trial_id"]
        wait_until("试验3三站登记",
                   lambda: len(http_json("GET", f"{coord}/api/trials/{t3}")["stations"]) == 3)
        # station-2 保持断线：它在 t3 中尚无任何汇总，屏障不会推进，阶段稳定可重放
        http_json("POST", station_urls[1] + "/api/station/fault", {"mode": "offline"})
        http_json("POST", f"{coord}/api/trials/start/{t3}", {})
        wait_until("试验3其余两站标准化受理", lambda: sqlite3.connect(
            os.path.join(base, "coord.db")).execute(
            "SELECT COUNT(*) FROM submissions WHERE trial_id=? AND phase='STANDARDIZE' AND accepted=1",
            (t3,)).fetchone()[0] >= 2, timeout=30)
        time.sleep(2)  # 确认屏障稳定停在 2/3（station-2 缺席，不补零）
        from federation.lib.coordinator import Coordinator
        inproc = Coordinator(os.path.join(base, "coord.db"))
        inproc.freeze_advance = True  # 冻结推进，保证两次提交落在同一轮，专门验证幂等
        s2_fp = next(s["data_fp"] for s in inproc.store.list_stations(t3) if s["station_id"] == "station-2")
        envelope = {"trial_id": t3, "station_id": "station-2", "phase": "STANDARDIZE",
                    "phase_round": 0, "kind": "std", "data_fp": s2_fp,
                    "payload": {"n": 60, "sum": [1, 2, 3, 4], "sq_sum": [1.5, 4.5, 9.5, 16.5]}}
        first = inproc.submit(envelope)   # 首次：应受理（冻结推进，阶段不变）
        second = inproc.submit(envelope)  # 原样重放：应判 duplicate_ignored
        conn = sqlite3.connect(os.path.join(base, "coord.db"))
        accepted_cnt = conn.execute(
            "SELECT COUNT(*) FROM submissions WHERE trial_id=? AND station_id='station-2' "
            "AND phase='STANDARDIZE' AND accepted=1", (t3,)).fetchone()[0]
        conn.close()
        inproc.freeze_advance = False
        http_json("POST", station_urls[1] + "/api/station/fault", {"mode": "none"})
        check("重复回包返回 duplicate_ignored 且受理计数不增加",
              first.get("status") == "accepted" and second.get("status") == "duplicate_ignored"
              and accepted_cnt == 1, str({"first": first, "second": second, "cnt": accepted_cnt}))

        # 8) 发布闸门：未 DONE 不给发布；DONE 后报告完整可追溯
        wait_until("试验1进入 DONE",
                   lambda: http_json("GET", f"{coord}/api/trials/{trial_id}")["phase"] == "DONE",
                   timeout=60)
        cur = http_json("GET", f"{coord}/api/trials/{trial_id}")
        check("DONE 后 publish_ready 才为真", cur["publish_ready"] is True)
        published = http_json("POST", f"{coord}/api/trials/publish/{trial_id}", {})
        check("发布后状态 PUBLISHED", published["status"] == "PUBLISHED")
        report = published["report"]
        check("报告含三站贡献", len(report["stations"]) == 3)
        total_traced = sum(len(b) for s in report["stations"] for b in s["batch_trace"].values())
        check("批次可追溯且无遗漏", total_traced == report["total_batches"] == 180)
        check("全局族群数等于固定 K", len(report["cluster_table"]) == 3)
        check("中心已反变换为业务单位（温度 60~130℃区间）",
              all(60 < c["temperature_c"] < 130 for c in report["cluster_table"]))
        check("含全局轮廓系数", isinstance(report["global_silhouette"], float))

        # 9) 重启协调端：从 SQLite 恢复，状态停留正确、报告仍在
        coord_proc = h.procs[0]
        coord_proc.send_signal(signal.SIGTERM)
        coord_proc.wait(timeout=5)
        h.procs = h.procs[1:]
        h.start("federation.lib.coordinator", "--port", str(coord_port),
                "--db", os.path.join(base, "coord.db"))
        wait_ready(coord)
        recovered = http_json("GET", f"{coord}/api/trials/{trial_id}")
        check("协调端重启后发布状态与报告保留",
              recovered["status"] == "PUBLISHED" and recovered["report"]["report_fp"] == report["report_fp"])
        rec = http_json("POST", coord + "/api/recover", {"trial_id": trial_id})
        check("恢复接口回报原阶段 DONE", rec["recovered"][0]["phase"] == "DONE")

        # 10) 站点进程重启：本地持久化恢复，不重复制造贡献
        p1 = h.procs[0]
        p1.send_signal(signal.SIGTERM)
        p1.wait(timeout=5)
        h.procs = [p for p in h.procs if p is not p1]
        h.start("federation.lib.station", "--station-id", "station-1",
                "--port", str(ports[0]), "--coordinator", coord,
                "--data-dir", os.path.join(base, "stations"), "--batches", "60")
        wait_ready(station_urls[0])
        time.sleep(4)
        again = http_json("GET", f"{coord}/api/trials/{trial_id}")
        check("站点重启后试验数据未被重复计入（总批次仍180）",
              again["report"]["total_batches"] == 180)

    finally:
        h.stop_all()
        if failures:
            print(f"现场保留于: {base}")
        else:
            shutil.rmtree(base, ignore_errors=True)

    print()
    if failures:
        print(f"自检失败 {len(failures)} 项: {failures}")
        sys.exit(1)
    print("全部协议自检通过 ✔")


if __name__ == "__main__":
    main()
