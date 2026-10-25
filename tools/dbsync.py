"""表结构同步 CLI：模型定义即权威，DDL 自动对齐。

    python tools/dbsync.py                    # 同步（模型跟随装配的模块）
    python tools/dbsync.py --modules app,appx # 额外带上这些模块的 <pkg>.models
    python tools/dbsync.py --debug            # 打印每条 DDL
    python tools/dbsync.py --only User Order  # 仅同步指定模型（按类名）
    python tools/dbsync.py --drop             # 删除模型中已不存在的列（危险）
    python tools/dbsync.py --force            # 类型不兼容则重建列、清理非模型索引

同步范围 = ``--modules`` / ``WEBX_MODULES`` 指定的各模块的 ``<pkg>/models``，
         外加 ``webx/models``（未显式关掉内置路由时）。**没有模型清单配置项**：
         装配哪个模块就带上它的 models，改名成 ``_models.py`` 即可排除。

⚠️ 建表范围必须与服务进程一致。服务读 ``WEBX_MODULES``（见 run.py / server.sh），
所以这里默认也读它——两边同源，才不会出现「服务路由齐全但表没建」。
用 ``--modules`` 可以只对某几个模块建表（如按模块分库部署时）。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 先把项目根塞进 sys.path，才能 import webx / app（本仓库不装成包）
sys.path.insert(0, str(Path(__file__).resolve().parent))

import _bootstrap  # noqa: E402


async def _sync(args: argparse.Namespace) -> int:
    from webx.config import settings
    from webx.lib.schema_sync import SchemaSync

    models = settings.tortoise_config()["apps"]["models"]["models"]
    print(f"📦 模型模块: {', '.join(models)}")
    print(
        f"🗄️  数据库: {settings.DATABASE_NAME}@"
        f"{settings.DATABASE_HOST}:{settings.DATABASE_PORT}"
    )

    await SchemaSync.sync(
        only=args.only,
        force=args.force,
        drop_not_exist_col=args.drop,
        debug=args.debug,
        schema=args.schema or settings.DATABASE_SCHEMA,
    )

    # 认领热点索引不由 Tortoise 声明（混合排序方向表达不了），--force 同步还会把它当野索引清掉；
    # 同步完补一次，保证认领不会悄悄退化成全表扫描。幂等。
    if "webx.models" in models:
        from webx.services.lease import ensure_claim_index

        print(f"🔎 认领热点索引: {await ensure_claim_index()}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="webx 表结构同步")
    parser.add_argument(
        "--modules",
        default=None,
        help="逗号分隔的业务模块包名，如 app 或 app,appx；"
        "其 <pkg>.models 会并入同步范围（默认取环境变量 WEBX_MODULES）",
    )
    parser.add_argument("--only", nargs="*", default=None, help="仅同步指定模型（类名）")
    parser.add_argument("--force", action="store_true", help="类型不兼容则重建，清理非模型索引")
    parser.add_argument("--drop", action="store_true", help="删除模型中不存在的列（危险）")
    parser.add_argument("--debug", action="store_true", help="输出详细 DDL")
    parser.add_argument("--schema", default="", help="目标 schema（默认取配置）")

    _bootstrap.run(parser.parse_args(), _sync)


if __name__ == "__main__":
    main()
