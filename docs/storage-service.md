# 存储服务（webx_storage）

独立部署的文件存储节点。把 webx 的存储原语包成一个只服务于**服务间调用**的 HTTP 服务：
内部令牌鉴权、任意层级的对象键、可插拔的键策略、可选的保留期清理。

它与主站内置的 `/api/files` 是两回事：

| | 主站 `/api/files` | 存储服务 `webx_storage` |
|---|---|---|
| 调用方 | 登录用户 | 其它服务（task 服务、worker 的 uploader） |
| 鉴权 | 用户令牌 | `X-Internal-Token` |
| 键 | `<bucket>/<name>` 两段 | 任意层级，由键策略决定形状 |
| 部署 | 随主站 | 可单独部署到存储机器 |
| 依赖 | — | 复用 webx 的存储原语、签名、应用工厂；**webx 核心不依赖它** |

## 部署

```bash
# 项目根放 .env（或用环境变量）。至少要配令牌，否则所有 /internal 请求都会被拒绝
STORAGE_INTERNAL_TOKEN=<随机长串>
STORAGE_SIGN_SECRET=<随机长串，可独立轮换>
STORAGE_DIR=/data/storage

python -m webx_storage --port 9010 --workers 4        # 等价于 sanic webx_storage.main:app
python -m webx_storage verify --url http://127.0.0.1:9010 --token "$STORAGE_INTERNAL_TOKEN"
```

多 worker 走 Sanic 官方 worker manager。续传会话的状态在磁盘上，多进程安全。

### 配置

| 键 | 默认 | 说明 |
|---|---|---|
| `STORAGE_DIR` | `storage` | 存储根目录 |
| `STORAGE_HOST` / `STORAGE_PORT` / `STORAGE_WORKERS` | `0.0.0.0` / `9010` / `1` | 监听 |
| `STORAGE_INTERNAL_TOKEN` | 回退 `INTERNAL_TOKEN` | 服务间令牌。**都为空则拒绝一切内部请求** |
| `STORAGE_SIGN_SECRET` | 回退 `TOKEN_SECRET`（并告警） | 签名密钥，建议独立配置以便轮换 |
| `STORAGE_SIGN_TTL_SECONDS` | `3600` | 签名默认有效期 |
| `STORAGE_POLICIES` | `task_results` | 启用的键策略，逗号分隔；置空 = 只提供通用接口 |
| `STORAGE_KEY_RULE` | `lenient` | `strict` / `lenient`，见下 |
| `UPLOAD_MAX_SIZE` | 2 GiB | 整传上限（回退 `STORAGE_REQUEST_MAX_SIZE`），更大的走分片 |
| `UPLOAD_CHUNK_MAX_BYTES` | 64 MiB | 单个分片上限 |
| `UPLOAD_SESSION_MAX_AGE_HOURS` | 24 | 未完成的分片会话存活上限，启动时回收 |
| `STORAGE_RETENTION_*` | 关闭 | 保留期清理，见下 |

## API

约定：**路径只放资源名词，动作由 HTTP 方法表达**。只有无法映射成 CRUD 的状态迁移，
才把动作名放在子路径（`/uploads/<id>/complete`）。详见 `docs/api-conventions.md`。

### 通用对象（内核，恒有）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/internal/objects` | 创建对象。multipart：`file` + `key`（表单字段，顺序不限）→ 201 `{key, size}` |
| GET / HEAD | `/internal/objects/<key>` | 读取；支持 `Range`（206/416）；HEAD 只回长度 |
| DELETE | `/internal/objects/<key>` | 删除，**幂等**（不存在也 200，`deleted:false`），顺带回收空目录 |
| POST | `/internal/uploads` | 创建分片会话 `{key, total?}` → 201 `{upload_id, offset:0, ...}` |
| GET | `/internal/uploads/<id>` | 查进度，响应头 `Upload-Offset` |
| PATCH | `/internal/uploads/<id>` | 追加分片，请求头 `Upload-Offset` 必须等于服务端已收字节数 |
| POST | `/internal/uploads/<id>/complete` | 完成，校验总长后原子落位 |
| DELETE | `/internal/uploads/<id>` | 放弃会话，幂等 |
| POST | `/internal/signatures` | 签发限时签名 `{key, ttl_seconds?}`，可在对象存在之前预签 |
| GET / HEAD | `/public/objects/<key>?sign=` | 凭签名下载，无需令牌；签名无效一律 404（不泄露存在性） |

分片协议（tus 的核心语义）：

```
POST  /internal/uploads {"key":"a/b.zip","total":N}      → {"upload_id":U,"offset":0}
PATCH /internal/uploads/U   Upload-Offset: 0   <bytes>   → 200, Upload-Offset: 8388608
      offset 不符 → 409，响应头 Upload-Offset = 服务端真实 offset，客户端据此重新对齐
GET   /internal/uploads/U                                → Upload-Offset 头（断线重连后先查）
POST  /internal/uploads/U/complete                       → {"key","size", ...}
```

### 任务产物策略 `task_results`

键布局 `tasks/<task_id>/results/<filename>`。"任务产物"在这里只是一种**键布局**，
存储服务不知道任务是什么。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/internal/tasks/<task_id>/results` | 创建结果文件（multipart，可选表单字段 `filename` 覆盖名字）→ 201 `{task_id, filename, download_url, size}` |
| POST | `/internal/tasks/<task_id>/results/uploads` | 创建分片会话 `{filename, total?}`；后续追加 / 完成走通用 `/internal/uploads/<id>`，完成时回 `download_url` |
| POST | `/internal/tasks/<task_id>/results/deletions` | 按 URL 批量删除 `{urls:[...]}`，幂等、逐条隔离、永不整体失败，`items` 与入参一一对应 |
| POST | `/internal/tasks/<task_id>/signatures` | 签发签名 `{filename, ttl_seconds?}` |
| DELETE | `/internal/tasks/<task_id>` | 删除整个任务目录，幂等 |
| DELETE | `/internal/tasks/<task_id>/results/<filename>` | 删除单个文件，幂等 |
| GET | `/public/tasks/<task_id>/results` | 列出文件 |
| GET / HEAD | `/public/tasks/<task_id>/results/<filename>` | 下载。**默认公开**；带 `?sign=` 则校验（无效 → 403）。`.log` 内联返回；`?name=` 指定下载名前缀 |

批量删除的两条安全约束：路径里的 `task_id` 是权威锚点，URL 里反解出的 task_id 必须与它相等，否则
该条标 `foreign_task`；URL 的反解只看末四段 `tasks/<id>/results/<name>`，反代前缀与查询串不参与判断。

**命名空间所有权**：`tasks/` 归 `task_results` 策略所有。通用接口不能写入或删除该命名空间
（403），读取不受限。否则通用接口写进去的对象会被该策略的保留期清理当成任务产物删掉。

### 兼容别名（已废弃）

仅为让既有 worker 的上传契约无需改动即可对接；响应带 `Deprecation: true` 与指向规范路径的 `Link` 头。

| 旧路径 | 规范路径 |
|---|---|
| `POST /internal/tasks/<id>/results/upload`（200） | `POST /internal/tasks/<id>/results`（201） |
| `POST /internal/tasks/<id>/results/upload/init` | `POST /internal/tasks/<id>/results/uploads` |
| `POST /internal/tasks/<id>/results/deletes` | `POST /internal/tasks/<id>/results/deletions` |
| `POST /internal/sign` `{task_id, filename}` | `POST /internal/tasks/<id>/signatures` |
| `POST /internal/objects/upload` | `POST /internal/objects` |

## 键规则与保留名

`STORAGE_KEY_RULE`：`strict` 只允许字母数字与 `_ - .`；`lenient` 允许中文、空格、括号，
以保留用户原始文件名。两种规则共同的底线：不得为空、不得是 `.` / `..`、不得含控制字符或 NUL、
单段不超过 255 字节、最多 16 段。**点开头的段永远是保留区**（`.uploads/` 会话、`.upload.*.tmp`
临时文件），既不能写也不会被列出。

⚠️ Sanic 不解码路径参数。服务端对 `<key:path>` 统一解码一次，**再**按键规则校验，
所以 `%2e%2e/` 这类编码过的穿越尝试和明文 `../` 一样被拒。客户端必须对路径做百分号编码。

## 扩展：键策略 `KeyPolicy`

存储服务本身只认对象键。业务形状通过策略接入：

```python
from webx_storage import KeyPolicy, register_policy

class Avatars(KeyPolicy):
    name = "avatars"
    def mount(self, ctx):                       # 追加自己的路由
        @ctx.public.get("/avatars/<uid:str>")
        async def avatar(request, uid): ...
    def owns(self, key): return key.startswith("avatars/")     # 独占命名空间（通用接口不可写）
    def describe_object(self, key, size): ...   # 给响应补字段，如 download_url
    def retention(self): ...                    # 可选：自己的清理方式

register_policy("avatars", Avatars)             # 然后 STORAGE_POLICIES=task_results,avatars
```

策略名写错会**启动即失败**（它决定对外暴露哪些路由，悄悄少一组比报错更难排查）。
下载名拼法等任务相关细节，继承 `TaskResultsPolicy` 覆盖 `download_name` 即可，不要改内核。

## 保留期清理（可选，默认关闭）

```bash
python -m webx_storage retention --dry-run     # 只列出将删除的条目
python -m webx_storage retention --once        # 跑一轮
STORAGE_RETENTION_ENABLED=true python -m webx_storage retention   # 常驻循环
```

- **独立进程**，不挂在应用里：存储服务多 worker 运行，应用内的后台任务会每个 worker 各跑一份，
  互相删对方正在检查的东西。
- 判据是目录树里**最新的 mtime**（还在被追加产物的任务不会被删）；只管辖策略声明的范围
  （`tasks/`），绝不碰其它区域。
- 单轮上限 `STORAGE_RETENTION_MAX_PER_ROUND`，一次配置失误删不空整个存储。
- `RetentionPolicy.confirm()` 是删前确认钩子。"该不该留"的权威方（通常是主站，它有容量、
  用量、购买时长）接进来后必须守住：**权威方不可达时绝不删**——查不到 ≠ 不存在，删除不可逆。
  纯 mtime 只是兜底，别把它当权威。

## 验证

三层，从快到慢：

1. **单元 / 集成测试**（进程内）：`pytest tests/test_storage_service.py tests/test_multipart_stream.py`
2. **真实进程测试**：`pytest tests/test_storage_live.py` —— 起 `python -m webx_storage` 本体，
   用验证脚本走真实 HTTP。
3. **部署验证**：对**线上 / 预发的真实服务**跑验证脚本：

   ```bash
   python -m webx_storage verify --url https://storage.example.com --token "$STORAGE_INTERNAL_TOKEN"
   ```

   覆盖：健康与鉴权、对象增删读与 Range、分片续传与 409 重同步、签名下载、`task_results` 全流程。
   能验出单测验不出的部署问题——反代没转发某些路径、网关吞了 `Range` / `Upload-Offset` 头、
   令牌配错、存储目录没写权限。测试数据写在 `__verify__/` 下，无论成败都清理。
   退出码 0 通过 / 1 有失败 / 2 服务不可达。

### 上线检查清单

- [ ] `STORAGE_INTERNAL_TOKEN`、`STORAGE_SIGN_SECRET` 已配置，且与调用方一致
- [ ] 反代转发了 `/internal`、`/public` 前缀，且**保留 `Range`、`Upload-Offset` 请求头与响应头**
- [ ] 反代对 `/internal` 做了来源限制（内部令牌是最后一道，不是唯一一道）
- [ ] `verify` 通过
- [ ] 保留期清理：确认没有意外启用（默认关闭；开启前先 `--dry-run`）
