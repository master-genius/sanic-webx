"""webx —— 面向纯 Web 服务的通用 Sanic 框架。

结构（自底向上，下层绝不 import 上层，由 tests/test_layering.py 焊死）：

    paths.py      项目根定位（叶子）
    config.py     BaseSettings（叶子）
    i18n.py       多语言文案（叶子，全框架共用）
    lib/          令牌、密码哈希、表结构同步、权限注册表、发送通道、短信、存储
    models/       user / admin / verify_code / rbac / dtos
    services/     user / admin / verifycode / permission / storage
    middleware/   可插拔中间件，一件一文件（含认证 / RBAC / CORS）
    core/         应用工厂、蓝图扫描、ORM、异常、日志、路由级守卫
    controller/   内置路由（auth / api / admin / public），与业务项目同一套 __mid__.py 机制

四件核心能力：

  1. 自建加密令牌   webx.lib.token        AES-256-GCM，零第三方依赖
  2. ORM 同步机制   webx.lib.schema_sync  模型即权威，最小 DDL 对齐
  3. 自动蓝图路由   webx.core.auto_blueprint
  4. 层级中间件     webx.middleware/      一个中间件一个模块，__mid__.py 挂载

快速开始见 README.md；仓库自带的 app/ 就是可运行的最小示例。
"""

__version__ = "0.1.0"
