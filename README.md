# 杉页阅读路径接续册

会话内一阶转移计数、条件概率与相对全局基率提升。为杉页研究协作组的12位分析与业务人员提供从样本核对、独立worker计算到批准快照交付的完整工作流。

## 真实业务场景

匿名章节阅读会话→路径接续关联→编辑衔接建议；仅会话内相邻，禁止跨会话拼接，非因果。团队原先将 `reading-transition-202611-01.csv` 复制到多份人工表格，复核时再逐项对照。三个痛点是口径不可追溯、重复计算易错、业务说明与结果脱节。上游提供合成业务观察，系统承担研究和复核，下游业务人员读取批准快照。全部演示数据是固定随机种子生成的模拟数据。

## 业务模块

1. 业务总览：批次、样本、任务、批准交付账册与日志导入账册。
2. 阅读文集：创建/修改来源说明、查看样本、用真实业务字段追加记录；列表分页。
3. 日志导入：百万行日志断点分批导入，按文件偏移续传，整批校验后一次发布。
4. 分析配置：冻结来源、参数和全部样本，进入Redis队列。
5. 完整结果：真实算法摘要、专属诊断、全部CSV导出和可恢复失败态。
6. 编辑衔接建议：分析师草案→负责人批准→阅读者下载批准快照。
7. 审计：登录、来源修改、导入分片/校验/发布、任务worker完成、业务说明批准等事件。

## 百万行日志断点导入

阅读日志只能分批追加，研究会收到尚未补齐的文件，导入链路按“接收→校验→发布”三段独立状态机设计（`import_jobs` 状态：`receiving / received / validating / validated / publishing / published / failed`）。

- **接收（内存≤128MB）**：前端按 4MB 分片、以文件偏移上传；后端用 1MB 循环缓冲 `pwrite` 按偏移落盘到共享卷 `imports/job-{id}/data.log`，内存占用与文件总大小无关。`import_chunks` 以 `(job_id, chunk_index)` 与 `(job_id, offset)` 双唯一键记账：重复块直接返回 `duplicate` 不重复写入；同偏移异值或同序号异值一律 409 拒收并把作业置为 `failed`。接收完成校验分片连续性与整文件 SHA256。
- **解析（内存≤128MB）**：`import_parser.py` 是字节级 RFC4180 流式解析器，每次只读 1MB 磁盘块。UTF-8 多字节（含中文）首字节不与引号/换行冲突，因此中文被任意切块、引号内换行跨块都能还原为同一条原记录；CRLF、孤立 CR、转义引号 `""` 均覆盖。实测 100 万行（31MB）解析约 6 秒、峰值 RSS 约 106MB。
- **坏行隔离**：字段数不符、引号未闭合、UTF-8 坏字节、业务校验失败均写入 `import_bad_rows`，保留 `line_no`、原始行 `raw_line` 与原因 `reason`，不阻断整批；空行跳过、表头自动识别。
- **整批失败口径**：同会话异路径（跨块同会话也算）或库内同会话异路径冲突使整批失败，冲突行连同原因进隔离区，不写入任何样本。完全重复行去重计数、不重复写入。
- **发布原子性**：合格样本先进 `import_staged_records` 暂存，对研究完全不可见；发布在单事务内按键集分页批量写入 `reading_sessions`，研究只见整批或完全不见。发布前再次核对库内冲突（导入期间允许手工追加样本与新建研究）。
- **可恢复**：worker 启动时清扫 `validating/publishing` 中断作业并重新入队；重新校验先清空旧暂存/隔离数据，重试不新增样本；发布崩溃回滚后重跑先清理同作业前缀残留。接收落盘与样本入库任一步中断都可从偏移/阶段继续。
- **页面**：“日志导入”页可查看接收进度、校验/发布阶段、合格/坏行/重复/冲突计数、整文件 SHA256、失败原因与隔离区原行。

## 快速启动

```bash
cp .env.example .env
docker compose config --quiet
docker compose up -d --build
# 可重复执行，不覆盖人工修改
docker compose exec -T backend python -m scripts.seed_demo
```

访问 [前端](http://localhost:18247)；[API文档](http://localhost:19247/docs)。数据库仅演示用途映射 `17747`。首次准备依赖和初始化数据库需要等待健康状态：`docker compose ps`，五个服务均需healthy。

| 组件 | 技术与职责 |
|---|---|
| frontend | 原生 HTML / CSS / JS ES Modules，Nginx代理 `/api/` |
| backend | Python3.11 / FastAPI / SQLAlchemy / JWT |
| worker | 独立Python进程，Redis BLPOP队列，持久化结果/CSV/心跳 |
| db | MySQL8，命名卷持久化 |
| redis | Redis7，AOF命名卷 |

恰好五服务，均配置healthcheck；后端/worker依赖数据库与Redis健康，前端依赖后端健康；worker与后端共享 `artifacts` 命名卷。重置演示环境：`docker compose down -v --remove-orphans`，会删除本项目卷。

## 账号与权限

所有演示账号密码：`Research2026!`。

| 邮箱 | 角色 | 权限 |
|---|---|---|
| admin@reading-transition.demo | admin | 全部录入/执行、批准业务说明 |
| analyst@reading-transition.demo | analyst | 录入来源/样本、执行、提交说明，无批准权 |
| viewer@reading-transition.demo | viewer | 只读、导出结果及已批准快照 |

时间在数据库/API中以UTC存储，总览与审计页面明确转换为Asia/Shanghai北京时间。

JWT有效期8小时；PBKDF2盐化哈希保存密码；角色每请求从数据库读取。访客写操作403，匿名访问业务数据401，重复样本409，非法业务字段422，重复批准409。每客户端/路径写请求90次/分钟，超限429。生产使用需修改JWT及演示数据库凭据。

## 实体与字段

ReadingCollection / ReadingSession / TransitionRun / EditorialPlan 是核心实体；另有User和Audit。数据库实际表：`reading_collections` → `reading_sessions` → `transition_runs` → `editorial_plans`。样本以固定字段JSON保存，后端按以下字段严格验证，不接受未知字段替代必填值：匿名会话编号（`session`）、章节路径（用 > 分隔）（`path`）。

实体贯穿 `backend/src/models`、`schemas`、`services`、`controllers`、`routes`，以及 `frontend/src/api/research.js`、`stores/research.js` 和 `pages`。`Source.key` 与 `(Record.source_id, reference)` 为自然键；来源改名不会修改已冻结任务输入。业务说明在创建时保存结果快照，批准后从该快照导出。

## 算法与口径

会话内一阶转移计数、条件概率与相对全局基率提升，全部计算位于 `backend/src/services/algorithm.py`，前端不拟合模型、不生成模拟结果。输入是冻结业务观察及参数，输出summary、专属诊断、逐行计算明细、seed与输入SHA256。CSV包含全部结果行；页面仅预览前30行。参数：

- `min_count`：最少接续次数，默认 `3`。
- `top_n`：保留接续规则数，默认 `20`。

只统计同一会话内的相邻章节；提升度相对于所有接续边的目标章节基率，关联不代表编辑调整造成的因果效果。

失败态：业务字段缺失/非有限数值、参数越界、样本不足、退化特征或结构错误均明确拒绝或将任务标failed，错误消息保存在任务中。可回到来源或配置修正后重新执行，历史失败记录保留。

已知边界：仅统计每行完整匿名会话内的相邻章节，拒绝会话编号重复；不是长序列规则挖掘。无跨会话拼接；提升度只是接续关联，不解释为因果。单章会话有效但不产生边。

## 演示数据与一天剧本

固定seed=42，3个来源 ` SY-202611-01/02/03`，每来源240条，共720条业务记录；三个账号和首轮种子任务。自然键检查后仅插入缺失数据，连跑seed不重复、不覆盖用户修改。`SEED_DEMO=true` 时启动自动执行；失败记录日志但不阻塞服务。首轮任务同样由独立worker真实计算。

1. 09:00 负责人查看三份来源和720条样本。
2. 09:10 分析师选择 `SY-202611-01 秋季常规组`，核对 `reading-transition-202611-01.csv`。
3. 09:15 在“阅读文集”核对匿名会话编号（`session`）、章节路径（用 > 分隔）（`path`），追加编号 `SY-NEW-001` 的样本。
4. 09:20 新建“十一月复核研究”，按默认参数冻结241条输入。
5. 09:21 独立worker消费Redis队列，返回实算结果与输入校验和。
6. 09:25 分析师查看专属诊断及逐行结果，并下载CSV复算。
7. 09:30 提交“十一月业务复核意见”，负责人核对并批准。
8. 09:40 viewer下载批准快照，查看审计中的worker完成与批准事件。

## 目录与本地开发

```text
frontend/src/api, stores, types, components/common, hooks, pages, router, utils, constants
backend/src/routes, controllers, services, models, schemas, middlewares, utils, constants, config
backend/scripts/seed_demo.py
backend/migrations/001_initial.sql
backend/tests/test_parser.py, test_import_flow.py, test_perf_million.py
```

导入相关代码：`models/import_*.py`（作业/分片/隔离行/暂存样本四表）、`services/import_parser.py`（字节级流式解析）、`services/imports.py`（接收与偏移续传）、`services/import_processing.py`（worker 校验/发布/中断清扫）、`routes/imports.py`、`pages/imports.js`。

本地后端：Python3.11虚拟环境安装 `backend/requirements.txt`，设置 `DATABASE_URL`、`REDIS_URL`、`ARTIFACT_DIR`，在backend目录运行 `uvicorn src.main:app --reload`；另开终端运行 `python -m src.worker`。原生前端无需编译，推荐以Compose Nginx访问，保证 `/api/` 代理。前端为Google Fonts双字体、oklch色板，离线字体回退serif；1280/768/390响应布局。

| 环境变量 | 默认与说明 |
|---|---|
| COMPOSE_PROJECT_NAME | `reading-transition`，隔离容器/网络/命名卷 |
| FRONTEND_PORT / BACKEND_PORT / DB_PORT | `18247 / 19247 / 17747` |
| SEED_DEMO | `true` |
| JWT_SECRET | `.env.example`演示值，部署前修改 |
| DATABASE_URL / REDIS_URL | Compose内部连接，定义在compose的env锚点 |
| ARTIFACT_DIR | `/artifacts`，后端worker共享 |
| IMPORT_DIR | `${ARTIFACT_DIR}/imports`，断点导入落盘目录（job-{id}/data.log） |
| RECEIVE_CHUNK_BYTES / PARSE_BLOCK_BYTES | 均为 `1048576`（1MB），接收写盘与解析读块的流式缓冲上限 |
| IMPORT_DB_BATCH | `2000`，暂存/发布每批落库条数；MAX_LINE_BYTES 单逻辑行上限 10MB |

共享枚举：`backend/src/constants/enums.py` 与 `frontend/src/constants/enums.js` 都定义 admin/analyst/viewer、queued/running/completed/failed、draft/approved。后端service检查对应字符串；README为全量值说明。共享前端组件table/status/chart/business跨总览、结果、配置与复核复用；actions hook和format utils负责交互/格式。后端errors与rate_limit是两个中间件。

测试及截图在批根 `test-artifacts/gb-0043/`，不混入源码。独立算法oracle、HTTP完整流程和真实Playwright浏览器流程见该目录 `TESTING.md`。License：MIT。
