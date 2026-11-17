"""跨厂联合试验：共享协议、口径与算法（仅依赖 Python 标准库）。

设计要点：
- 站点间只交换标准化/分群所需汇总（均值平方和、加权中心、计数、批次标签摘要），
  原始批次记录永不离站；
- 所有汇总带数据指纹与轮次号，协调端据此做幂等、拒收旧轮迟到回包；
- 算法确定性：固定种子 42 的 k-means++，任何站点/协调端重算结果一致。
"""
from __future__ import annotations

import hashlib
import json
import math
import random

FEATURES = ["temperature_c", "pressure_bar", "residence_min", "yield_pct"]
FEATURE_LABELS = ["反应温度(℃)", "釜压(bar)", "停留时间(min)", "收率(%)"]
RANDOM_SEED = 42
# 试验阶段（顺序推进，不允许跳阶段/跳轮）
PHASES = ["STANDARDIZE", "INIT", "ITERATE", "FINAL", "DONE"]
PHASE_LABELS = {
    "STANDARDIZE": "标准化协商",
    "INIT": "初始中心协商",
    "ITERATE": "迭代分群",
    "FINAL": "归群定稿",
    "DONE": "已发布",
}
# 试验生命周期状态
TRIAL_CREATED = "CREATED"          # 已创建，未启动（参数仍可改）
TRIAL_RUNNING = "RUNNING"          # 启动后固定参与站/数据/K
TRIAL_PUBLISHED = "PUBLISHED"      # 全部就绪并发布
TRIAL_ABORTED = "ABORTED"          # 终止（保留审计）


def canonical(payload) -> bytes:
    """规范化 JSON 字节，保证指纹/签名在各进程间一致。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def fingerprint(payload) -> str:
    """汇总包内容指纹（sha256 前 16 位），用于幂等与追溯。"""
    return hashlib.sha256(canonical(payload)).hexdigest()[:16]


def data_fingerprint(records) -> str:
    """站点本地数据指纹：仅暴露批次号与数值的哈希，原始数据不离站。"""
    digest = hashlib.sha256()
    for row in records:
        digest.update(canonical([row["batch_id"]] + [row[f] for f in FEATURES]))
    return digest.hexdigest()[:16]


# ------------------------- 口径校验（与 BUSINESS.md 一致） -------------------------

def validate_records(records, k: int):
    """站点本地数据入库前的失败态校验，不通过则拒绝参加试验。"""
    if len(records) < max(30, k * 5):
        raise ValueError("至少需要30条记录，且每个候选族群平均至少5条。")
    matrix = []
    for row in records:
        try:
            vector = [float(row[f]) for f in FEATURES]
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("温度、釜压、停留时间、收率须完整且为数值。") from exc
        matrix.append(vector)
    for vector in matrix:
        if not all(math.isfinite(v) for v in vector):
            raise ValueError("工况须为有限数值。")
        if vector[1] <= 0 or vector[2] <= 0:
            raise ValueError("釜压与停留时间须为正数。")
        if not 0 <= vector[3] <= 100:
            raise ValueError("收率须在0~100之间。")
    for j in range(len(FEATURES)):
        if max(r[j] for r in matrix) == min(r[j] for r in matrix):
            raise ValueError("四个特征均须有变化，存在恒定特征。")
    distinct = {tuple(v) for v in matrix}
    if len(distinct) < k:
        raise ValueError("不同工况数量不足 K 种。")
    return matrix


# ------------------------- 标准化（全局口径由各站汇总合成） -------------------------

def local_std_summary(matrix):
    """站点标准化汇总：只交计数、各特征和与平方和，不交原始行。"""
    n = len(matrix)
    sums = [0.0] * len(FEATURES)
    sq_sums = [0.0] * len(FEATURES)
    for vector in matrix:
        for j, value in enumerate(vector):
            sums[j] += value
            sq_sums[j] += value * value
    return {"n": n, "sum": [round(s, 8) for s in sums],
            "sq_sum": [round(s, 8) for s in sq_sums]}


def aggregate_std(summaries):
    """协调端合并各站汇总，得到全局均值/标准差（总体标准差，与 StandardScaler 同口径）。"""
    n = sum(s["n"] for s in summaries)
    sums = [0.0] * len(FEATURES)
    sq_sums = [0.0] * len(FEATURES)
    for s in summaries:
        for j in range(len(FEATURES)):
            sums[j] += s["sum"][j]
            sq_sums[j] += s["sq_sum"][j]
    means = [s / n for s in sums]
    stds = []
    for j in range(len(FEATURES)):
        variance = max(sq_sums[j] / n - means[j] ** 2, 0.0)
        stds.append(math.sqrt(variance) if variance > 1e-18 else 1.0)
    return {"n": n, "mean": means, "std": stds}


def standardize(matrix, std_params):
    """各站用全局参数在本地做 Z 标准化。"""
    mean, std = std_params["mean"], std_params["std"]
    return [[(v - mean[j]) / std[j] for j, v in enumerate(row)] for row in matrix]


def inverse_centers(centers, std_params):
    """标准化中心反变换回业务单位。"""
    mean, std = std_params["mean"], std_params["std"]
    return [[c[j] * std[j] + mean[j] for j in range(len(FEATURES))] for c in centers]


# ------------------------- 确定性 KMeans（k-means++，种子固定 42） -------------------------

def _squared_dist(a, b):
    return sum((x - y) ** 2 for x, y in zip(a, b))


def kmeans_pp_init(points, k, rng):
    """加权 k-means++ 初始化；points 可带权重（协调端合并候选中心时使用）。"""
    weights = None
    if points and isinstance(points[0], tuple):
        weights = [p[1] for p in points]
        pts = [p[0] for p in points]
    else:
        pts = points
    first_idx = rng.choices(range(len(pts)), weights=weights, k=1)[0]
    centers = [pts[first_idx][:]]
    closest = [_squared_dist(p, centers[0]) for p in pts]
    while len(centers) < k:
        probs = [d * (weights[i] if weights else 1.0) for i, d in enumerate(closest)]
        total = sum(probs)
        if total <= 0:  # 退化：均匀抽一个不同点
            idx = rng.randrange(len(pts))
        else:
            idx = rng.choices(range(len(pts)), weights=probs, k=1)[0]
        centers.append(pts[idx][:])
        for i, p in enumerate(pts):
            closest[i] = min(closest[i], _squared_dist(p, centers[-1]))
    return centers


def assign(points, centers):
    """返回每个点的簇号与各点到中心的平方距离。"""
    labels, distances = [], []
    for p in points:
        dists = [_squared_dist(p, c) for c in centers]
        label = min(range(len(centers)), key=lambda i: dists[i])
        labels.append(label)
        distances.append(dists[label])
    return labels, distances


def kmeans(points, k, init_centers=None, max_iter=100, tol=1e-6, seed=RANDOM_SEED):
    """标准 Lloyd 迭代；返回中心、标签、惯性值与最大中心位移历史。"""
    rng = random.Random(seed)
    centers = [c[:] for c in (init_centers or kmeans_pp_init(points, k, rng))]
    shifts = []
    for _ in range(max_iter):
        labels, distances = assign(points, centers)
        new_centers = []
        for c in range(k):
            members = [p for p, lab in zip(points, labels) if lab == c]
            if members:
                new_centers.append([sum(dim) / len(members) for dim in zip(*members)])
            else:  # 空簇：取距当前中心最远的点，保证中心数恒为 K
                far = max(range(len(points)), key=lambda i: distances[i])
                new_centers.append(points[far][:])
        shift = math.sqrt(max(_squared_dist(a, b) for a, b in zip(new_centers, centers)))
        centers = new_centers
        shifts.append(shift)
        if shift <= tol:
            break
    labels, distances = assign(points, centers)
    inertia = sum(distances)
    return centers, labels, inertia, shifts


def silhouette(points, labels, k):
    """轮廓系数 (b-a)/max(a,b)，本地计算，只交换聚合值。样本过大时抽样上界 2000。"""
    n = len(points)
    if n <= k or len(set(labels)) < 2:
        return 0.0
    sample_idx = list(range(n))
    if n > 2000:
        rng = random.Random(RANDOM_SEED)
        sample_idx = sorted(rng.sample(sample_idx, 2000))
    scores = []
    for i in sample_idx:
        same = [j for j in sample_idx if labels[j] == labels[i] and j != i]
        a = sum(math.sqrt(_squared_dist(points[i], points[j])) for j in same) / len(same) if same else 0.0
        b = math.inf
        for cluster in set(labels):
            if cluster == labels[i]:
                continue
            others = [j for j in sample_idx if labels[j] == cluster]
            if others:
                b = min(b, sum(math.sqrt(_squared_dist(points[i], points[j])) for j in others) / len(others))
        if math.isinf(b):
            continue
        scores.append(0.0 if max(a, b) == 0 else (b - a) / max(a, b))
    return sum(scores) / len(scores) if scores else 0.0


def local_iteration_summary(z_points, centers):
    """站点迭代汇总：按广播中心本地归群，只交各簇加权向量和与计数。"""
    labels, _ = assign(z_points, centers)
    k = len(centers)
    dim = len(FEATURES)
    vec_sum = [[0.0] * dim for _ in range(k)]
    counts = [0] * k
    for p, lab in zip(z_points, labels):
        counts[lab] += 1
        for j in range(dim):
            vec_sum[lab][j] += p[j]
    return {"labels": labels, "counts": counts,
            "vec_sum": [[round(v, 8) for v in row] for row in vec_sum]}


def merge_iteration(summaries, k):
    """协调端加权合并各站汇总得到新一轮全局中心（缺席站不补零，屏障未齐不推进）。"""
    dim = len(FEATURES)
    vec_sum = [[0.0] * dim for _ in range(k)]
    counts = [0] * k
    for s in summaries:
        for c in range(k):
            counts[c] += s["counts"][c]
            for j in range(dim):
                vec_sum[c][j] += s["vec_sum"][c][j]
    centers = []
    for c in range(k):
        if counts[c] == 0:
            raise ValueError(f"簇{c}在已交汇总中无样本，无法合成全局中心。")
        centers.append([vec_sum[c][j] / counts[c] for j in range(dim)])
    return centers, counts
