/* 跨厂联合分群试验台：纯原生 JS，无构建依赖。 */
const $ = (sel, root = document) => root.querySelector(sel);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

const PHASES = ["STANDARDIZE", "INIT", "ITERATE", "FINAL", "DONE"];
const PHASE_LABEL = {
  STANDARDIZE: "标准化协商", INIT: "初始中心", ITERATE: "迭代分群", FINAL: "归群定稿", DONE: "已发布",
};
let state = { trials: [], selected: null, timer: null };

async function api(method, path, body) {
  const resp = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);
  return data;
}

function toast(msg, isErr = false) {
  const el = $("#toast");
  el.textContent = msg;
  el.className = "show" + (isErr ? " err" : "");
  setTimeout(() => (el.className = ""), 3200);
}

function fmtTime(ms) {
  if (!ms) return "-";
  return new Date(ms).toLocaleTimeString("zh-CN", { hour12: false });
}

// ---------------- 建试验 / 启动 ----------------
$("#form-trial").addEventListener("submit", async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const body = {
    name: fd.get("name"),
    k: Number(fd.get("k")),
    max_rounds: Number(fd.get("max_rounds")),
    tol: Number(fd.get("tol")),
    station_urls: fd.get("station_urls").split(/\s+/).map((s) => s.trim()).filter(Boolean),
  };
  try {
    const trial = await api("POST", "/api/trials", body);
    toast(`试验 ${trial.trial_id} 已创建，等待各站登记`);
    await refresh();
  } catch (err) {
    toast(err.message, true);
  }
});

$("#btn-start-selected").addEventListener("click", async () => {
  const created = state.trials.find((t) => t.status === "CREATED");
  if (!created) return toast("没有处于 CREATED 状态的试验", true);
  try {
    await api("POST", `/api/trials/start/${created.trial_id}`, {});
    toast("试验已启动：参与站、数据与 K 已固定");
    await refresh();
  } catch (err) {
    toast(err.message, true);
  }
});

$("#btn-refresh").addEventListener("click", refresh);
$("#auto-refresh").addEventListener("change", (e) => {
  clearInterval(state.timer);
  if (e.target.checked) state.timer = setInterval(refresh, 2000);
});
$("#btn-close-detail").addEventListener("click", () => {
  state.selected = null;
  $("#detail-card").classList.add("hidden");
});

// ---------------- 试验列表 ----------------
function renderList() {
  const el = $("#trial-list");
  if (!state.trials.length) {
    el.innerHTML = '<p class="hint">尚无试验。先在上方创建，站点会在数秒内自动登记。</p>';
    return;
  }
  el.innerHTML = state.trials.map((t) => {
    const roundText = t.phase === "ITERATE" ? `第 ${t.phase_round} 轮` :
      t.phase === "DONE" ? `共 ${(t.round_shifts || []).length} 轮` : `阶段轮次 ${t.phase_round}`;
    return `
    <div class="trial-row">
      <div>
        <div class="title">${esc(t.name)} <span class="mono">${esc(t.trial_id)}</span></div>
        <div class="trial-meta">
          <span>状态 <b class="badge ${t.status}">${t.status}</b></span>
          <span>阶段 <b>${PHASE_LABEL[t.phase]}</b>（${roundText}）</span>
          <span>K=${t.k}</span>
          <span>屏障 ${t.barrier_got}/${t.expected_stations}</span>
          <span>创建 ${fmtTime(t.created_at)}</span>
        </div>
      </div>
      <div class="row-actions">
        <button class="btn small" data-act="detail" data-id="${t.trial_id}">详情/进度</button>
        ${t.status === "CREATED" ? `<button class="btn small primary" data-act="start" data-id="${t.trial_id}">启动</button>` : ""}
        ${t.publish_ready && t.status === "RUNNING" ? `<button class="btn small primary" data-act="publish" data-id="${t.trial_id}">批准发布</button>` : ""}
        ${t.status === "RUNNING" ? `<button class="btn small danger" data-act="abort" data-id="${t.trial_id}">终止</button>` : ""}
      </div>
    </div>`;
  }).join("");
  el.querySelectorAll("button[data-act]").forEach((btn) =>
    btn.addEventListener("click", () => rowAction(btn.dataset.act, btn.dataset.id)));
}

async function rowAction(act, id) {
  try {
    if (act === "detail") {
      state.selected = id;
      await renderDetail();
      return;
    }
    if (act === "start") await api("POST", `/api/trials/start/${id}`, {});
    if (act === "publish") {
      await api("POST", `/api/trials/publish/${id}`, {});
      toast("已发布：全局中心与各站归群全部就绪，可供评审简报");
    }
    if (act === "abort") {
      const reason = prompt("终止原因（审计留痕）", "人工终止");
      if (reason === null) return;
      await api("POST", `/api/trials/abort/${id}`, { reason });
    }
    await refresh();
    if (state.selected) await renderDetail();
  } catch (err) {
    toast(err.message, true);
  }
}

// ---------------- 详情：阶段条、屏障、站点失败、审计、结果 ----------------
function phaseStrip(t) {
  const idx = PHASES.indexOf(t.phase);
  return `<div class="phase-strip">${PHASES.map((p, i) => {
    const cls = i === idx ? "active" : i < idx ? "done" : "";
    const round = p === "ITERATE" && t.round_shifts ? `（已合并 ${t.round_shifts.length} 轮）` : "";
    return `<div class="phase-step ${cls}">${PHASE_LABEL[p]}${round}</div>`;
  }).join("")}</div>`;
}

function stationPanel(t) {
  return `<div class="station-grid">${t.stations.map((s) => `
    <div class="station-card">
      <div class="sid">${esc(s.station_id)}<span class="pill ${s.current_round_state}">
        ${{ accepted: "本轮已交", absent: "缺席（不计零）", rejected: "被拒收", finalized: "归群已定稿" }[s.current_round_state] || s.current_round_state}
      </span></div>
      <div class="mono" style="margin:4px 0">${esc(s.base_url)}</div>
      <div>批次 ${s.n_batches} · 指纹 <span class="mono">${esc(s.data_fp)}</span></div>
    </div>`).join("")}</div>
  <p class="barrier">同轮屏障：${t.barrier_got}/${t.expected_stations} 站汇总齐备才推进；
  缺席站保持等待，绝不以零贡献顶替。
  <progress value="${t.barrier_got}" max="${t.expected_stations}"></progress></p>`;
}

function shiftChart(t) {
  const shifts = t.round_shifts || [];
  if (!shifts.length) return '<p class="hint">尚无迭代位移记录。</p>';
  const max = Math.max(...shifts, 1e-12);
  return `<div class="shift-chart">${shifts.map((v, i) =>
    `<div class="bar2" style="height:${Math.max(6, (v / max) * 100)}%" title="第${i + 1}轮 位移 ${v}"><span>${i + 1}</span></div>`).join("")}</div>
  <p class="hint">收敛阈值 tol=${t.tol}；${esc(t.stop_reason || "迭代进行中")}</p>`;
}

function auditPanel(t) {
  return `<details open><summary>审计轨迹（最近 ${t.audit_tail.length} 条，含拒收/重复/恢复）</summary>
  <div class="audit">${t.audit_tail.map((a) => `
    <div class="ev"><span class="t">${fmtTime(a.created_at)}</span><b>${esc(a.event)}</b>
    ${a.station_id ? ` <span class="pill rejected" style="margin:0">${esc(a.station_id)}</span>` : ""}
    ${a.detail ? `<code>${esc(a.detail)}</code>` : ""}</div>`).join("")}</div></details>`;
}

function reportPanel(t) {
  if (!t.report) {
    return `<div class="gate">⚠ 半成品闸门：各站归群与全局中心尚未全部就绪，当前结果不供批准或简报。</div>`;
  }
  const r = t.report;
  const trace = r.stations.map((s) => `
    <details><summary>${esc(s.station_id)}（${s.n_batches} 批，本地轮廓 ${s.silhouette}，指纹 ${esc(s.data_fp)}）</summary>
      <table><tr><th>族群</th><th class="num">本站批次数</th></tr>
      ${Object.entries(s.clusters).map(([c, n]) => `<tr><td>${esc(c)}</td><td class="num">${n}</td></tr>`).join("")}
      </table>
      <div class="trace-list">${Object.entries(s.batch_trace).map(([c, ids]) =>
        `<b>${esc(c)}</b>：${ids.map(esc).join("，")}`).join("<br>")}</div>
    </details>`).join("");
  return `
  <div class="gate ready">✔ 发布就绪：报告指纹 <span class="mono">${esc(r.report_fp)}</span>，
  全局加权轮廓系数 <b>${r.global_silhouette}</b>，共 ${r.total_batches} 批，来源 ${r.stations.length} 个站点。</div>
  <h3>全局族群中心（已反变换回业务单位，按温度排序，标签无优劣含义）</h3>
  <table>
    <tr><th>族群</th><th class="num">批次数</th><th class="num">占比%</th>
      ${r.feature_labels.map((l) => `<th class="num">${esc(l)}</th>`).join("")}</tr>
    ${r.cluster_table.map((c) => `<tr><td>${esc(c.cluster)}</td><td class="num">${c.count}</td>
      <td class="num">${c.share_pct}</td>
      ${["temperature_c", "pressure_bar", "residence_min", "yield_pct"].map((f) =>
        `<td class="num">${c[f]}</td>`).join("")}</tr>`).join("")}
  </table>
  <h3>各站贡献与批次追溯</h3>${trace}
  <p class="hint">停止原因：${esc(r.stop_reason)}；轮次位移序列：${(r.round_shifts || []).map((v) => v.toExponential(2)).join(" → ")}</p>`;
}

async function renderDetail() {
  const t = state.trials.find((x) => x.trial_id === state.selected);
  if (!t) return;
  $("#detail-card").classList.remove("hidden");
  $("#d-trial-id").textContent = t.trial_id;
  const publishBtn = t.publish_ready && t.status === "RUNNING"
    ? `<button class="btn primary" id="d-publish">批准发布（全部就绪后可用）</button>` : "";
  $("#detail-body").innerHTML = `
    <div class="kv">
      <div><b>状态</b>${t.status}</div>
      <div><b>当前阶段</b>${PHASE_LABEL[t.phase]} / 轮 ${t.phase_round}</div>
      <div><b>固定 K</b>${t.k}（启动后锁定）</div>
      <div><b>配置指纹</b><span class="mono">${esc(t.config_fp)}</span></div>
      <div><b>参与站</b>${t.expected_stations} 个（固定）</div>
      <div><b>收敛</b>${t.converged ? "是" : "否"}</div>
    </div>
    ${phaseStrip(t)}
    ${stationPanel(t)}
    <h3>迭代中心位移（收敛轨迹）</h3>${shiftChart(t)}
    ${reportPanel(t)}
    ${auditPanel(t)}
    <div class="row-actions">
      <button class="btn warn" id="d-recover">协调端恢复/催交缺席站（原轮继续，不跳轮）</button>
      ${publishBtn}
    </div>`;
  const rec = $("#d-recover");
  if (rec) rec.addEventListener("click", async () => {
    try {
      const r = await api("POST", "/api/recover", { trial_id: t.trial_id });
      toast("恢复完成：" + JSON.stringify(r.recovered[0]));
      await refresh();
      await renderDetail();
    } catch (err) { toast(err.message, true); }
  });
  const pub = $("#d-publish");
  if (pub) pub.addEventListener("click", () => rowAction("publish", t.trial_id));
}

// ---------------- 站点故障模拟控制台 ----------------
function renderStationConsole() {
  const urls = new Set();
  state.trials.forEach((t) => (t.station_urls || []).forEach((u) => urls.add(u)));
  const el = $("#station-console");
  if (!urls.size) {
    el.innerHTML = '<p class="hint">创建试验后，此处可对各本地站点注入故障。</p>';
    return;
  }
  el.innerHTML = [...urls].sort().map((u) => `
    <div class="console-row">
      <span class="url">${esc(u)}</span>
      <select data-url="${esc(u)}" class="fault-mode">
        <option value="none">正常</option>
        <option value="offline">断线（拒绝出站，轮询失败待重试）</option>
        <option value="duplicate">重复回包（验证幂等不重复计入）</option>
        <option value="delay">延迟回包（4 秒慢响应）</option>
      </select>
      <button class="btn small" data-act="fault" data-url="${esc(u)}">注入</button>
      <button class="btn small" data-act="stale" data-url="${esc(u)}">制造旧轮迟到回包</button>
      <button class="btn small" data-act="retry" data-url="${esc(u)}">站点重试 pending</button>
    </div>`).join("");
  el.querySelectorAll("button[data-act]").forEach((btn) =>
    btn.addEventListener("click", () => stationAction(btn)));
}

async function stationAction(btn) {
  const url = btn.dataset.url;
  const action = btn.dataset.act;
  const running = state.trials.find((t) => t.status === "RUNNING") || state.trials[0];
  try {
    if (action === "fault") {
      const mode = $(`.fault-mode[data-url="${CSS.escape(url)}"]`).value;
      await api("POST", "/api/proxy/station/", { station_url: url, action: "fault", mode, note: "页面模拟" });
      toast(`已向 ${url} 注入故障：${mode}`);
    } else if (action === "stale") {
      if (!running) throw new Error("需要先有试验");
      const r = await api("POST", "/api/proxy/station/", {
        station_url: url, action: "stale", trial_id: running.trial_id,
      });
      toast("迟到回包响应：" + JSON.stringify(r), false);
    } else if (action === "retry") {
      const r = await api("POST", "/api/proxy/station/", {
        station_url: url, action: "retry", trial_id: running ? running.trial_id : null,
      });
      toast("重试结果：" + JSON.stringify(r.retried || []));
    }
    await refresh();
  } catch (err) {
    toast(err.message, true);
  }
}

// ---------------- 刷新循环 ----------------
async function refresh() {
  try {
    const data = await api("GET", "/api/trials");
    const keepSelected = state.selected;
    state.trials = data.trials;
    renderList();
    renderStationConsole();
    if (keepSelected && state.trials.some((t) => t.trial_id === keepSelected)) {
      state.selected = keepSelected;
      await renderDetail();
    }
  } catch (err) {
    toast("协调端不可达：" + err.message, true);
  }
}

refresh();
state.timer = setInterval(refresh, 2000);
