"""同步测试里操作真实 PostgreSQL 的小工具。

HTTP 端到端用例是同步的（Sanic 测试客户端自带事件循环），不能在测试函数里直接 ``await`` ORM；
这里每个调用开一个独立的事件循环 + ORM 生命周期，用完即关。
"""

from __future__ import annotations

import asyncio

import asyncpg
from tortoise import Tortoise

from tests.conftest import TEST_MODELS, reset_orm_caches


def run(coro):
    return asyncio.run(coro)


async def with_orm(dsn: str, fn):
    """在一个临时 ORM 上下文里执行异步函数 ``fn()``。"""
    reset_orm_caches()  # 同进程里 sqlite 与 PG 混用时，模型缓存的 SQL 带着旧方言
    await Tortoise.init(db_url=dsn, modules={"models": TEST_MODELS})
    try:
        return await fn()
    finally:
        await Tortoise.close_connections()


def orm(dsn: str, fn):
    return run(with_orm(dsn, fn))


def fetch(dsn: str, sql: str, *args):
    async def go():
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.fetch(sql, *args)
        finally:
            await conn.close()

    return run(go())


def execute(dsn: str, sql: str, *args):
    async def go():
        conn = await asyncpg.connect(dsn)
        try:
            return await conn.execute(sql, *args)
        finally:
            await conn.close()

    return run(go())


def reset_schema(dsn: str) -> None:
    """清空库：drop + create schema。表由被测应用启动时的 generate_schemas 重建。"""
    execute(dsn, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;")


def ensure_schema(dsn: str) -> None:
    """建表与认领索引（测试里要直接调用服务层函数时用）。"""
    from webx.models.task import Task

    async def go():
        await Tortoise.generate_schemas(safe=True)
        await Tortoise.get_connection("default").execute_query(Task.CLAIM_INDEX_DDL)

    orm(dsn, go)
