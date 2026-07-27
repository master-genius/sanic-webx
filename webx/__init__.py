"""webx —— 面向纯 Web 服务的通用 Sanic 框架。

结构（与业务项目对称）：

    core/         配置、路径、蓝图扫描、中间件、应用工厂、ORM、i18n、日志
    middleware/   可插拔中间件，一件一文件
    lib/          令牌、密码哈希、表结构同步、权限注册表、发送通道、短信
    models/       user / admin / verify_code / rbac / dtos
    services/     user / admin / verifycode / permission
    controller/   内置路由（auth / api / admin），与业务项目同一套 __mid__.py 机制

四件核心能力：

  1. 自建加密令牌   webx.lib.token        AES-256-GCM，零第三方依赖
  2. ORM 同步机制   webx.lib.schema_sync  模型即权威，最小 DDL 对齐
  3. 自动蓝图路由   webx.core.auto_blueprint
  4. 层级中间件     webx.core.middleware + webx.middleware/

快速开始见 README.md 与 examples/minimal/。
"""

__version__ = "0.1.0"
