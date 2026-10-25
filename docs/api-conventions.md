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
   如 `/tasks/<id>/cancel`、`/uploads/<id>/complete`、`/leases/<id>/release`。
   判断标准：它是对已有资源的一次**状态变化**，且没有合适的名词能代替。

4. **批量操作用名词化的子资源**，而不是动词：`POST /results/deletions`（创建一次删除请求）、
   `POST /signatures`（创建一个签名）。不用带请求体的 `DELETE`——网关会丢请求体。

5. **登录、注册这类"会话 / 账号创建"也按资源建模**：`POST /sessions`、`POST /accounts`。
   存量接口按风险分批调整，见「存量排查」。

6. **幂等**：服务间接口的 `DELETE` 对不存在的目标也回 `200`，用 `deleted: false` 告知；
   调用方（常常是 fire-and-forget）不需要区分。面向用户的接口对不存在的目标回 `404`。

7. **状态码**：创建 `201`；成功 `200`；参数错误 `400`；未认证 `401`；无权限 `403`；
   不存在 `404`；冲突 `409`；过大 `413`；限流 `429`；上游故障 `502`。
   **公开端点对"无权访问"也回 `404`**，不泄露资源是否存在。

8. **错误体**统一 `{"error": "<说明>"}`；需要机器可读原因时加 `code` / `reason`。

9. **兼容别名**：旧路径需要保留时，作为别名挂在规范路径旁，响应带
   `Deprecation: true` 与 `Link: <规范路径>; rel="successor-version"`，文档里标"已废弃"。
   别名**只做转发**，不得有自己的逻辑分支。

10. **路径参数不被框架解码**（Sanic 原样传入），处理器入口统一用
    `webx.services.storage_http.decode_param` 解一次，**再**做合法性校验。

## 存量排查

（在统一排查阶段填写。）
