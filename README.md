# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

支持**可选紧急等级**（`priority`，默认 `normal` 普通）：机房突发故障时，
`urgent` 紧急请求在两条连接都忙的情况下可越过先入队的普通请求优先获取
共享连接。调度只调整**等待顺序**，绝不中断正在执行的 SQL；只要仍有普通
请求在等，连续放行最多两条紧急请求后必须放行一条普通请求（防饥饿）。
等待期间到期或被撤销（如客户端断开）的请求不占用名额、不发送 SQL，后续
请求仍得到自身结果。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志与只读角色 `fault_ro`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000,
  "priority": "normal"
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。
`priority` 可选，取值 `normal`（默认）或 `urgent`；非法值由 Pydantic
返回 422。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，附 `priority`、`wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| 排队期间请求被撤销（客户端断开） | 499 | `error.code=acquire_cancelled`，语句**从未发送**、不占名额 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |
| 紧急等级非法 | 422 | 请求体校验失败 |

### `GET /health`

返回连接池容量、空闲/占用数、普通/紧急排队数、连续紧急放行计数、累计创建
与弃置数。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用进程内优先级等待队列 + 借用者计数把**物理后端数**
限制为 2：只有当实际存在空闲后端（`borrowers < backends`）时才放行等待者，
第三个请求排队等待而非新开连接——即使发生过弃置补建，物理连接也绝不超过
两条。`acquire(deadline, priority, cancel_event)` 接收绝对单调时钟截止
时间，等待与执行共用同一预算；排队期间过期或被撤销则直接离开队列（503 /
499），不发送任何语句、不占用名额，调度器立刻把名额转给下一个等待者。

### 2. 紧急优先与防饥饿调度

等待者按 `(priority, 入队序号)` 管理，同等级严格 FIFO：

- 两条连接都忙时，`urgent` 请求越过先入队的 `normal` 请求先获得连接；
- 只重排**等待中**的请求，已经在后端上执行的 SQL 不会被中断；
- 只要队列里还有普通请求，连续放行 2 条紧急请求后（`URGENT_BURST_LIMIT`），
  下一个空闲名额强制给普通请求；普通请求全部离开后计数器复位；
- 到期的等待者在每轮调度前先被清除，不可能被放行、不可能发送 SQL。

### 3. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 4. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，调用 libpq `PQcancel`（经独立取消连接）请求服务器中止语句。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 5. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。若补建暂时失败，
池临时缩水并在下一次获取时懒建补回，不会死锁。

### 6. 排队中撤销

HTTP 客户端在排队阶段断开时，`Request.is_disconnected()` 监听任务置位
撤销事件，等待中的请求以 499 离开队列；一旦已经拿到连接开始执行，则按
正常请求跑完（不中断 SQL）。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn
python -m tests.selftest         # 端到端自测（56 项）
python tests/test_protocol.py    # 协议直测：超时/迟到结果/排队/优先级/撤销（27 项）
```

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色（Compose 首次初始化执行）
tests/          selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```
