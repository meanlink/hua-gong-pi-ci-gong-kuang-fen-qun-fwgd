"""本地演示启停：一个协调端 + 三个本地站点，全部仅依赖标准库。

用法：
  python -m federation.run_demo start   # 启动协调端(8060)与站点(8071~8073)
  python -m federation.run_demo status  # 查看各进程与健康状态
  python -m federation.run_demo stop    # 停止全部进程
  python -m federation.run_demo logs    # 打印最近日志
启动后浏览器打开 http://127.0.0.1:8060/
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

from .lib.web import http_json

ROOT = os.path.dirname(os.path.abspath(__file__))
RUN_DIR = os.path.join(ROOT, ".run")
DATA_DIR = os.path.join(ROOT, "data")
COORD_PORT = 8060
STATIONS = [("station-1", 8071), ("station-2", 8072), ("station-3", 8073)]


def _pid_path(name):
    return os.path.join(RUN_DIR, f"{name}.pid")


def _log_path(name):
    return os.path.join(RUN_DIR, f"{name}.log")


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def start():
    os.makedirs(RUN_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    procs = []
    coord_log = open(_log_path("coordinator"), "w")
    coord = subprocess.Popen(
        [sys.executable, "-m", "federation.lib.coordinator",
         "--port", str(COORD_PORT), "--db", os.path.join(DATA_DIR, "coordinator.db")],
        cwd=os.path.dirname(ROOT), stdout=coord_log, stderr=subprocess.STDOUT)
    _write_pid("coordinator", coord.pid)
    procs.append(("coordinator", coord))
    for station_id, port in STATIONS:
        log = open(_log_path(station_id), "w")
        proc = subprocess.Popen(
            [sys.executable, "-m", "federation.lib.station",
             "--station-id", station_id, "--port", str(port),
             "--coordinator", f"http://127.0.0.1:{COORD_PORT}",
             "--data-dir", os.path.join(DATA_DIR, "stations"), "--batches", "60"],
            cwd=os.path.dirname(ROOT), stdout=log, stderr=subprocess.STDOUT)
        _write_pid(station_id, proc.pid)
        procs.append((station_id, proc))
    time.sleep(1.5)
    print("已启动：")
    for name, proc in procs:
        print(f"  {name:10s} pid={proc.pid} alive={proc.poll() is None}")
    print(f"\n试验台页面: http://127.0.0.1:{COORD_PORT}/")
    print("健康检查: ", end="")
    try:
        print(http_json("GET", f"http://127.0.0.1:{COORD_PORT}/health"))
    except Exception as exc:
        print(f"协调端尚未就绪: {exc}（稍后重试或查看 logs）")


def _write_pid(name, pid):
    with open(_pid_path(name), "w") as fh:
        fh.write(str(pid))


def stop():
    for name in ["coordinator", *[s[0] for s in STATIONS]]:
        path = _pid_path(name)
        if not os.path.exists(path):
            continue
        pid = int(open(path).read().strip())
        if _alive(pid):
            os.kill(pid, signal.SIGTERM)
            print(f"已停止 {name} (pid={pid})")
        os.remove(path)


def status():
    for name in ["coordinator", *[s[0] for s in STATIONS]]:
        path = _pid_path(name)
        if not os.path.exists(path):
            print(f"{name:10s} 未运行")
            continue
        pid = int(open(path).read().strip())
        print(f"{name:10s} pid={pid} alive={_alive(pid)}")
    try:
        trials = http_json("GET", f"http://127.0.0.1:{COORD_PORT}/api/trials")["trials"]
        print(f"\n协调端在管试验 {len(trials)} 个:")
        for t in trials:
            print(f"  {t['trial_id']} {t['status']:9s} {t['phase']:11s} "
                  f"轮{t['phase_round']} 屏障{t['barrier_got']}/{t['expected_stations']} {t['name']}")
    except Exception as exc:
        print(f"协调端不可达: {exc}")


def logs():
    for name in ["coordinator", *[s[0] for s in STATIONS]]:
        path = _log_path(name)
        print(f"===== {name} =====")
        if os.path.exists(path):
            with open(path) as fh:
                lines = fh.readlines()
            print("".join(lines[-15:]))
        else:
            print("（无日志）")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    {"start": start, "stop": stop, "status": status, "logs": logs}.get(cmd, status)()
