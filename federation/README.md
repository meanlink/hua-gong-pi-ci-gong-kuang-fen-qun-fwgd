# 跨厂联合分群试验（联邦 KMeans）

在不允许原始工况离站的前提下，支持多个分厂联合完成标准化与 KMeans 迭代分群。
各站保留原始批次台账，站点间只交换**标准化汇总、加权中心和批次归群标签摘要**，
最终结果可追溯到每个站点的每一批次及其贡献。

整个子系统仅使用 Python 标准库（`http.server` + `sqlite3`），无第三方依赖、无需容器。

## 目录结构

```
federation/
├── lib/
│   ├── protocol.py     # 共享协议：指纹、口径校验、标准化、确定性kmeans、轮廓系数、汇总合并
│   ├── store.py        # 协调端 SQLite(WAL) 持久化：试验/站点/汇总(唯一约束)/审计
│   ├── coordinator.py  # 协调端：固定参与站、屏障推进、轮次校验、发布闸门、恢复
│   ├── station.py      # 本地站点：原始数据不出站、轮询提交、故障注入、本地持久化
│   └── web.py          # 标准库 HTTP 工具
├── webui/              # 试验台页面（原生 HTML/CSS/JS，无构建）
├── selftest.py         # 20 项端到端协议自检（临时目录自动拉起/销毁全部进程）
└── run_demo.py         # 本地一键启停（协调端 8060，站点 8071~8073）
```

## 快速开始

```bash
python -m federation.run_demo start     # 启动协调端 + 3 个本地站点
# 浏览器打开 http://127.0.0.1:8060/
python -m federation.run_demo status    # 查看进程与在管试验
python -m federation.run_demo logs      # 查看日志
python -m federation.run_demo stop      # 停止全部
python -m federation.selftest           # 端到端自检（退出码 0 即全部通过）
```

页面操作流程：① 建试验（K 2~6、收敛阈值、固定站点 URL）→ 站点数秒内自动登记 →
② 点"启动"（参与站、数据指纹、K 立即冻结）→ 观察阶段条与同轮屏障 →
收敛后 ③ 点"批准发布"查看全局中心与各站批次追溯。

## 交换了什么、不交换什么

| 阶段 | 站点上报内容（汇总） | 永不离站 |
|---|---|---|
| 标准化 | 计数 `n`、各特征和、平方和 | 原始温/压/时/收率行 |
| 初始中心 | 本地 k-means++ 候选中心 + 各簇计数、惯性 | 标准化后的逐条样本 |
| 迭代分群 | 各簇加权向量和 `vec_sum` 与计数 | 逐条样本与批次工况值 |
| 归群定稿 | 每批 `batch_id` + 簇标签、本地轮廓系数 | 原始工况值（标签摘要不含数值） |

协调端由各站和/平方和合成全局均值标准差（与 `StandardScaler` 同口径的总体标准差），
广播全局中心；站点本地归群后只回加权和，协调端按计数加权合并得到新一轮中心。
最终中心反变换回业务单位，按温度排序显示，标签无优劣含义。

## 协议保证（与需求逐条对应）

1. **启动即固定**：`start` 时快照参与站集合、各站数据指纹与 K，写入 `config_fp`；
   之后新增/替换站点、变更指纹一律拒绝（见 `register_station`/`submit`）。
2. **同轮屏障**：每个阶段/轮次按固定站点数计数，`got < expected` 绝不推进，
   缺席站保持 `absent`，不以零贡献顶替。
3. **重试不重复计入**：`submissions` 表对 `(试验,阶段,轮次,站点,类型)` 建唯一约束，
   重复提交返回 `duplicate_ignored`，受理计数不增加；站点侧也有本地幂等状态。
4. **中断恢复原轮**：全部状态落 SQLite(WAL)；协调端重启执行 `recover_on_startup`
   重评当前轮屏障，站点重启从本地 `state` 表恢复，已受理项不重发，不跳轮。
5. **旧轮迟到拒收**：回包 `phase/phase_round` 与当前轮不符即 `rejected_stale`，
   写 `stale_rejected` 审计，不入库计入。
6. **并发不跳轮**：校验-入库-屏障检查-阶段推进在同一把全局锁内原子完成，
   迭代轮次号只在锁内 `+1`。
7. **发布闸门**：所有站点 FINAL 归群摘要齐备才生成报告并进入 DONE；
   `publish` 再次校验站点数与批次台账一致，半成品状态下页面不提供批准入口，
   报告含 `report_fp` 指纹、全局中心、各站贡献与逐批追溯。

## 故障模拟（页面"本地站点故障模拟"面板）

- `offline`：切断到协调端的出站（站点本地控制面仍可达，模拟带外管理口），轮询失败留待重试；
- `duplicate`：每次受理后自动再发一份相同回包，验证幂等；
- `delay`：回包延迟 4 秒，验证慢响应下屏障仍正确；
- "制造旧轮迟到回包"：按上一轮编号重发，协调端应拒收并审计；
- "站点重试 pending"：手工触发待发汇总重投。

## 主要 HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/trials` | 创建试验（CREATED，参数可改） |
| POST | `/api/trials/start/{id}` | 固定参与站/数据/K 并启动 |
| POST | `/api/station/register` | 站点登记（带数据指纹与批次数） |
| POST | `/api/station/submissions` | 汇总提交（幂等、轮次校验、指纹校验） |
| GET  | `/api/station/assignments?station_id=` | 站点轮询当前轮上下文 |
| POST | `/api/recover` | 协调端恢复并催交缺席站 |
| POST | `/api/trials/publish/{id}` | 发布闸门通过后发布 |
| POST | `/api/proxy/station/` | 页面对站点故障/重试/迟到的带外代理 |
| GET  | `/api/trials`、`/api/trials/{id}`、`/api/trials/audit/{id}` | 列表/详情/审计 |

## 与集中式口径的一致性

特征、失败态校验、Z 标准化、轮廓系数公式、按温度排序标签等口径与
`backend/src/services/business.py` 保持一致；k-means 使用固定种子 42 的
k-means++ 与 Lloyd 迭代，任何进程重算结果确定可复现。
