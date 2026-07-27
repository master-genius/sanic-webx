"""表结构同步 CLI：模型定义即权威，DDL 自动对齐。

    python tools/dbsync.py           # 同步
    python tools/dbsync.py --debug   # 打印每条 DDL
    python tools/dbsync.py --only User Order
    python tools/dbsync.py --drop    # 删除模型中已不存在的列（危险）
    python tools/dbsync.py --force   # 类型不兼容则重建列、清理非模型索引
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
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="webx 表结构同步")
    parser.add_argument("--only", nargs="*", default=None, help="仅同步指定模型（类名）")
    parser.add_argument("--force", action="store_true", help="类型不兼容则重建，清理非模型索引")
    parser.add_argument("--drop", action="store_true", help="删除模型中不存在的列（危险）")
    parser.add_argument("--debug", action="store_true", help="输出详细 DDL")
    parser.add_argument("--schema", default="", help="目标 schema（默认取配置）")

    _bootstrap.run(parser.parse_args(), _sync)


if __name__ == "__main__":
    main()
