# API 设计规范

适用于 webx 核心、`webx_storage`、任务模块的所有 HTTP 接口。排查结果见文末「存量排查」。

## 规则

1. **路径放资源名词，动作由 HTTP 方法表达。**

   | 意图 | 方法 | 例 |
   |---|---|---|
   | 创建 | `POST` 集合 | `POST /internal/objects` |
   | 读取 | `GET` 单个 / 集合 | `GET /internal/objects/<key>` |
   | 整体替换 | `PUT` | |
   | 局部修改 | `PATCH` | `PATCH /internal/uploads/<id>`（追加分片） |
   | 删除 | `DELETE` | `DELETE /internal/objects/<key>` |

2. **路径里不出现 `create / upload / delete / remove / get / list / add / update / set` 这类动词。**
   `POST` 本身就是创建，`DELETE` 本身就是删除。
   ✗ `POST /results/upload`　✓ `POST /results`
   ✗ `DELETE /files/delete/<id>`　✓ `DELETE /files/<id>`

3. **无法映射成 CRUD 的状态迁移，才把动作名放在子路径**：`POST /<资源>/<id>/<动词>`，
   如 `/tasks/<id>/cancel`、`/uploads/<id>/complete`。（租约归还没有用 `/leases/<id>/release`，而是 `DELETE /internal/tasks/<id>/lease`——它是对租约这个名词的删除，可以映射成 CRUD。）
   判断标准：它是对已有资源的一次**状态变化**，且没有合适的名词能代替。

4. **批量操作用名词化的子资源**，而不是动词：`POST /results/deletions`（创建一次删除请求）、
   `POST /signatures`（创建一个签名）。不用带请求体的 `DELETE`——网关会丢请求体。

5. **登录、注册这类"会话 / 账号创建"也按资源建模**：`POST /sessions`、`POST /accounts`。
   存量接口按风险分批调整，见「存量排查」。

6. **幂等**：服务间接口的 `DELETE` 对不存在的目标也回 `200`，用 `deleted: false` 告知；
   调用方（常常是 fire-and-forget）不需要区分。面向用户的接口对不存在的目标回 `404`。

7. **状态码**：创建 `201`；成功 `200`；参数错误 `400`；未认证 `401`；无权限 `403`；
   不存在 `404`；冲突 `409`；过大 `413`；限流 `429`；上游故障 `502`。
   **公开端点对"无权访问"也回 `404`**，不泄露资源是否存在。例外：任务产物的公开读取（`/public/tasks/<id>/results/...`）
   签名无效回 `403`，见 `storage-service.md`。

8. **错误体**统一 `{"error": "<说明>"}`；需要机器可读原因时加 `code` / `reason`。

9. **兼容别名**：旧路径需要保留时，作为别名挂在规范路径旁，响应带
   `Deprecation: true` 与 `Link: <规范路径>; rel="successor-version"`，文档里标"已废弃"。
   别名**只做转发**，不得有自己的逻辑分支。

10. **路径参数不被框架解码**（Sanic 原样传入），处理器入口统一用
    `webx.services.storage_http.decode_param` 解一次，**再**做合法性校验。

## 存量排查

范围：webx 核心（`auth` / `api` / `admin` / `public` / `internal` 五个内置组）、`webx_storage`、`task_server`，
共 110 条路由（Web 服务 82 + 存储服务 25 + 领取服务 3，含兼容别名与探活，按方法 × 路径计，不含 HEAD / OPTIONS）。
判据是上面的 10 条规则；业务示例模块 `app` 的 4 条（`/api/notes` ×3、`/public/welcome`）不在框架范围内。

### 本次已改（旧路径保留为废弃别名，只转发，带 `Deprecation: true` 与 `Link: <规范路径>; rel="successor-version"`）

| 旧 | 规范 | 为什么 |
|---|---|---|
| `POST /api/files/<bucket>/<filename>/sign` | `POST /api/files/<bucket>/<filename>/signatures` | `sign` 是动词；签名是被创建出来的东西，按规则 4 名词化，也与存储服务的 `/internal/signatures` 一致 |
| `GET/PATCH/DELETE /api/files/uploads/<id>`<br>`POST /api/files/uploads/<id>/complete` | `/api/uploads/<id>`<br>`/api/uploads/<id>/complete` | 分片会话是独立资源（会话 id 已携带目标键），不该挂在 bucket 之下。旧路径与 `/api/files/<bucket>/<filename>` 同形，**名为 `uploads` 的 bucket 里的文件下载会被会话路由遮住**——这是实际缺陷，不只是风格问题 |
| `POST /api/admin/account/<id>/passwd` | `PUT /api/admin/account/<id>/passwd` | 设置子资源的值是 `PUT`，与 `PUT /api/user/passwd` 一致；权限判定不变 |

创建分片会话仍是 `POST /api/files/<bucket>/uploads`（“在这个 bucket 里创建一个上传”，名词，保持不变）。

**别名的下线条件**：确认没有调用方后删除，一并解除上面“名为 `uploads` 的 bucket”的遮蔽——在别名存在期间，
`GET /api/files/uploads/<x>` 仍会命中会话别名而不是 bucket 里的文件（按规范路径 `/api/files/<bucket>/<name>` 以外的写法无法区分）。
这是保留别名的已知代价。

### 本次新增的接口：已按规范设计，无需改动

- 任务：`/api/tasks`（集合 + `<id>`）、`PATCH /<id>`、`PUT /<id>/info`、子资源 `/children` `/files`；
  状态迁移 `submit / cancel / retry / revise / redo` 是对已有任务的状态变化，没有合适名词，按规则 3 保留动词。
- 内部回调：`/internal/tasks/<id>/{heartbeats,fields,steps,deliveries,files}` 全是名词；`status` 用 `PUT`；
  `complete` / `fail` / `ack` 是状态迁移；归还租约是 `DELETE .../lease`（路径里没有 delete / release）。
- 领取服务：`POST /task/leases`（创建一次领取）、`POST /task/pokes`（创建一次唤醒）。
- 存储服务：`/internal/objects`、`/internal/uploads`、`/internal/signatures` 规范；兼容别名见 `storage-service.md`。

### 评估后**有意不改**（有风险，仅记录）

| 路由 | 问题 | 不改的原因 | 建议 |
|---|---|---|---|
| `POST /auth/login` `/auth/admin-login` `/auth/logout` `/auth/refresh` `/auth/register` | 按规则 5 应建模为 `POST /auth/sessions`、`DELETE /auth/sessions`、`POST /accounts` | 登录 / 注册 / 登出是几乎所有前端与第三方都按字面写死的路径，且鉴权中间件的白名单、限流、审计都按路径配置；收益只是风格，风险是全站登录失效 | 若要改，随一次大版本一起做，旧路径别名保留一个完整版本周期 |
| `POST /auth/verifycode` `/auth/verifycode-login` `/auth/account/change-by-code` | 路径里带动作（`change-by-code` 最重） | 同上，且 `change-by-code` 同时承载找回密码与换绑，拆分需要业务确认 | 同上 |
| `POST /api/user/{email,mobile}/bind` `/unbind` `/bind/code` | `bind` 应为 `PUT /api/user/email`；`unbind` 应为 `DELETE /api/user/email` | 解绑要求在请求体里带当前密码，而 `DELETE` 带请求体会被不少网关丢弃（规则 4）；只改 bind 不改 unbind 会把成对接口拆成两种风格 | 改成 `PUT` + “解绑 = `POST /api/user/email/removals`（名词化）”前先确认前端 |
| `DELETE /internal/tasks/<task_id>`（存储服务） | 语义是“删除该任务的全部产物”，路径却只有任务 id，与 Web 服务里的 `/internal/tasks/<id>` 同形不同义 | 这是旧存储契约的兼容路径，调用方（`storage_client.delete_task`）已固定 | 新调用方用 `POST /internal/tasks/<id>/results/deletions` |
| `PUT /api/tasks/<id>/info` 与 `PATCH /api/tasks/<id>` | 两条编辑路径容易混淆 | `info` 只改展示用基本信息，`PATCH` 改参数并推进状态机，权限与副作用不同 | 文档里已区分；如需合并，先统一副作用模型 |
| `GET /`、`GET /health` | 无 | 约定俗成的探活路径 | — |

### 仍存在的别名

**存储服务**（已在 `storage-service.md` 列出对照表）：`/internal/objects/upload`、`/internal/sign`、
`/internal/tasks/<id>/results/{upload,upload/init,deletes}`。它们让既有 worker 的上传契约无需改动即可对接，只转发，
响应带 `Deprecation` 与 `Link`（`{task_id}` 等占位符按本次请求的路径参数展开）。

**Web 服务**：`POST /api/files/<b>/<f>/sign`、`/api/files/uploads/<id>[/complete]`、`POST /api/admin/account/<id>/passwd`，见上表“本次已改”。

`/public/tasks/<id>/results[/<file>]` **不是别名**，是任务产物策略直接注册的规范公开路由。

