# taskflow

本地文件状态的任务编排服务（任务队列 + DAG 工作流）。运行时只用 Python 3.11
标准库，不连外部服务、不起数据库，全部状态落在 `state_dir` 里。

## 用法

```bash
python3 -m taskflow run    --config config.json              # 合并 DAG 并跑到结束
python3 -m taskflow status --config config.json              # 输出 JSON 状态，可给脚本读
python3 -m taskflow retry  --config config.json --task <id>  # 把死信/失败任务重新排队
```

- `run`：全部任务成功退出码为 0；有任务最终失败、进死信（或被挡住、被停机打断）退出码非 0。
- `retry` 只负责把任务重新排队（并自动解除下游的 blocked、重置已尝试次数，
  重放的任务享受完整的退避/重试额度），随后再执行一次 `run` 即可重放。

## 配置（config.json）

```json
{
  "state_dir": "./state",
  "concurrency": 4,
  "retry": {"base_seconds": 1.0, "max_attempts": 3},
  "task_timeout_seconds": 3600,
  "dag_file": "./dag.json"
}
```

- `state_dir`：状态目录（`state.json`、锁文件、每个任务的日志都在这里）。
- `concurrency`：同时运行的任务数上限，严格不超过。
- `retry.base_seconds` / `retry.max_attempts`：退避基数（秒）与最大尝试次数，
  第 n 次失败后的等待时间为 `base_seconds * 2^(n-1)`。
- `task_timeout_seconds`：可选，单次尝试的超时秒数（>0）。超时视为本次尝试失败，
  按退避规则重试、耗尽后进死信；超时的任务进程及其派生的整组子进程都会被 SIGKILL
  收拾干净，不允许同一任务出现两份实例。缺省/为 null 表示不限制。
- `dag_file`：任务集合文件。相对路径一律相对于配置文件所在目录解析。

## 任务集合（dag.json）

```json
{
  "tasks": [
    {"id": "extract", "run": "python3 extract.py", "needs": [],
     "idempotency_key": "batch-2026-09-19-extract"},
    {"id": "load", "run": "python3 load.py", "needs": ["extract"]}
  ]
}
```

- `run` 是一条 shell 命令（用 `shell=True` 执行），退出码非 0 视为失败。
- `needs` 是上游任务 id 列表，可为空；所有上游成功后才调度，上游进死信则本任务标记为 `blocked`。
  依赖必须指向本批次或 state_dir 中已存在的任务 id；指向不存在的 id 会让整批提交
  当场报错退出（并列出缺失的 id），绝不会静默丢弃依赖后把下游跑掉。
- `idempotency_key` 可选；同一个 key 重复提交（哪怕换了任务 id）不会执行第二遍。

## 任务状态

`pending`（等依赖）→ `running` → `succeeded`；失败时进入 `failed`（等待退避重试），
重试耗尽后进入 `dead`（死信）；上游死信导致永远不可运行的任务是 `blocked`。
`status` 输出每个任务的状态、已尝试次数、开始/结束时间（UTC ISO8601），进程重启后不丢。

## 可靠性保证

- **不丢任务**：每次状态变迁都原子写盘（临时文件 + `os.replace` + `fsync`）。
- **kill -9 / 断电恢复**：重启后 `running` 的任务重置为 `pending` 接着跑，
  `failed` 的按持久化的重试时间继续退避，`succeeded` 的绝不重跑。
- **优雅停机**：收到 SIGTERM/SIGINT 后不再调度新任务，在手任务跑完、状态落盘后再退出。
  被中断的一轮退出码非 0（哪怕在手任务刚好成功），避免部署脚本把没跑完的数据当成成功。
- **超时治理**：`task_timeout_seconds` 到时后整个进程组被 SIGKILL 并完成回收，
  按普通失败走退避/重试/死信；崩溃恢复时也会先清掉残留的旧进程组，再重新调度，
  保证同一任务同一时刻最多只有一份实例在跑。
- **防并发冲突**：同一 `state_dir` 上有 flock 文件锁，两个 `run` 不会同时操作一份状态。

## 测试

```bash
python3 -m pytest -q
```

覆盖：强杀恢复（不重跑已成功任务、续跑中断任务）、幂等键去重、死信与手工重放、
SIGTERM 优雅停机、并发上限。
