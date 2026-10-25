# 文档索引

按“你要做什么”找：

| 我想… | 看这个 |
|---|---|
| 部署、起服务、配反代、装 systemd 单元 | [`../deploy/README.md`](../deploy/README.md) |
| 上线后确认系统真的通了；了解每一层测试在证明什么 | [`verification.md`](verification.md) |
| 跑测试、准备 PostgreSQL | [`testing.md`](testing.md) |
| 给 API 起名 / 评审接口设计；查某条旧路径为什么没改 | [`api-conventions.md`](api-conventions.md)（含存量排查） |
| 对接任务接口（创建、提交、状态机、注册任务类型、配额与点数接缝） | [`task-api.md`](task-api.md) |
| 理解租约、认领、fencing、补偿器、领取服务的设计与不变量 | [`task-scheduling.md`](task-scheduling.md) |
| 写 / 改 worker，或自己实现一个兼容的客户端 | 协议：[`worker-protocol.md`](worker-protocol.md)；实现与配置：[`worker.md`](worker.md) |
| 用存储服务、写键策略、配保留期清理 | [`storage-service.md`](storage-service.md) |
| 框架本身（自动蓝图、中间件、令牌、配置、短信） | 仓库根 [`../README.md`](../README.md) |

## 文档之间的权威关系

- **worker 与服务端之间的行为，以 `worker-protocol.md` 为准。** `worker.md` 里的“协议缺口的裁定”记录了实现过程中发现的歧义及其结论，
  结论都已回写到协议文档；两处不一致时先怀疑 `worker.md` 过期。
- **路径命名以 `api-conventions.md` 为准**，其“存量排查”记录了每条被保留的旧路径及理由。
- 数字和路径会随代码变，文档里凡是写了“N 条路由 / 某个测试名”的地方，改代码时一并 grep。
