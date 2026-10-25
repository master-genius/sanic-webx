# 测试

```bash
uv sync --extra dev
.venv/bin/python -m pytest -q          # 全量
.venv/bin/python -m pyflakes webx webx_storage task_server tools tests app run.py
```

## 需要 PostgreSQL 的测试

认领 / 租约的 SQL 是 PG 专有的（`FOR UPDATE SKIP LOCKED`、`now()`、`EXTRACT(EPOCH ...)`），sqlite 测不了，
而它们恰恰是全系统互斥性的来源，所以**不 mock 数据库**。涉及：`test_task_lease.py`、`test_reconciler.py`、
`test_task_server.py`。获取 PG 的方式，按优先级：

1. **你已有的实例**：`WEBX_TEST_PG_URL=postgres://user:pass@host:port/dbname`（库需为空，测试会建表并清空数据）。
2. **嵌入式 PG**（不需要 root 服务、不动系统）：

   ```bash
   mkdir -p ~/.cache/webx-pg && cd ~/.cache/webx-pg
   uv venv --python 3.12 pgenv                     # pgserver 暂无 3.13 的 wheel，只借它的 PG 二进制
   uv pip install --python pgenv/bin/python pgserver
   ```

   测试会自动找到 `~/.cache/webx-pg/pgenv/.../pginstall/bin`（也可用 `WEBX_PG_BIN` 指定任意含 `initdb` 的目录），
   每次测试会话拉起一个临时实例、结束后清理。以 root 运行时 PG 拒绝启动，会降权到 `pgserver` 用户
   （`WEBX_PG_USER` 可改）。
3. 都没有 → 这些测试**失败**并说明怎么办。**不会静默跳过**——跳过等于认领逻辑没被验证却显示绿色。
   确需跳过请显式设 `WEBX_SKIP_PG_TESTS=1`。

> 个别环境里 `/dev/null` 被误建成了普通文件（非 root 进程写它会 `Permission denied`，`initdb` 因此失败）。
> 测试夹具检测到时，会在**私有挂载命名空间**里给 PG 提供一个正常的 null 设备，不改宿主机。
> 根治办法是修复它：`rm /dev/null && mknod -m 666 /dev/null c 1 3`。

## 真实进程测试

`test_storage_live.py`、`test_task_server.py` 会起 `python -m webx_storage` / `python -m task_server` 本体，
走真实 HTTP。进程内的测试客户端每个请求都会起停一次应用，测不了"挂着的请求"、多 worker、入口参数翻译。
这些测试同时也是各自 `verify` 命令的回归测试。

## 变异验证

对关键防线，改坏它、确认对应的测试会失败、再还原——测试"能通过"不等于测试"能拦住回归"。
提交说明里会写明做过哪些。
