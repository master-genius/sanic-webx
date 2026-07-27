"""
webx/lib/schema_sync.py  —  NeoPG-style runtime schema sync for Tortoise-ORM
-----------------------------------------------------------------------------
Design philosophy: 模型定义即权威,information_schema 为真相,差异最小化为目标。
手动执行一次完成同步,不做运行时锁保护。

Index naming (deterministic, no randomness):
    {table}_{sorted_cols}_idx     regular index
    {table}_{sorted_cols}_uidx    unique index

Only indexes with these suffixes are "managed" — externally created indexes
are never touched (unless force=True).

Constraint vs Index distinction:
    PostgreSQL UNIQUE constraints (created by generate_schema) are backed
    by an index but must be removed via DROP CONSTRAINT, not DROP INDEX.
    This module detects constraint-backed indexes via pg_constraint and issues
    the correct DDL automatically.

Usage:
    # 手动同步 (开发环境)
    python -m tools.dbsync

    # 代码调用
    await Tortoise.init(config=TORTOISE_ORM)
    await SchemaSync.sync(debug=True)

    # 清理遗留 _key 约束（如从旧迁移工具过渡）
    await SchemaSync.migrate_from_aerich(debug=True)
    await SchemaSync.sync(force=True, debug=True)
"""

from __future__ import annotations

import logging
import re
from enum import Enum
from typing import Any

from tortoise import Tortoise
from tortoise.fields.data import CharEnumFieldInstance
from tortoise.indexes import Index as TortoiseIndex

logger = logging.getLogger("schema_sync")

# ---------------------------------------------------------------------------
# Postgres type map  (Tortoise field class name -> pg base type)
# ---------------------------------------------------------------------------
FIELD_TYPE_MAP: dict[str, str] = {
    "CharField":       "character varying",
    "TextField":       "text",
    "IntField":        "integer",
    "SmallIntField":   "smallint",
    "BigIntField":     "bigint",
    "FloatField":      "double precision",
    "DecimalField":    "numeric",
    "BooleanField":    "boolean",
    "DatetimeField":   "timestamp with time zone",
    "DateField":       "date",
    "TimeField":       "time without time zone",
    "JSONField":       "jsonb",
    "UUIDField":       "uuid",
    "BinaryField":     "bytea",
    # Enum fields store as text/int in Postgres
    "CharEnumField":   "character varying",
    "IntEnumField":    "integer",
}

NUMERIC_TYPES       = {"integer", "smallint", "bigint", "numeric", "double precision"}
STRING_TYPES        = {"character varying", "text", "character"}
TYPES_WITH_BRACKETS = {"character varying", "character", "numeric"}

# Managed-index suffixes — only indexes with these suffixes are touched
# in non-force mode. Externally created indexes are never modified.
_IDX  = "_idx"
_UIDX = "_uidx"


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------

def _make_index_name(table: str, cols: tuple[str, ...], unique: bool) -> str:
    """Deterministic name: {table}_{sorted_cols}_{_idx|_uidx}"""
    return f"{table}_{'_'.join(sorted(cols))}{'_uidx' if unique else '_idx'}"


def _is_managed(name: str) -> bool:
    """判断索引名是否由本工具管理（_idx / _uidx 后缀）"""
    return name.endswith(_IDX) or name.endswith(_UIDX)


# ---------------------------------------------------------------------------
# Field -> (pg_type, default, nullable, inferred)
#
# 只处理 fields_db_projection 中的真实 DB 列。
# FK 列在 projection 中已表现为 IntField/BigIntField (source_field)，无需特殊处理。
# ---------------------------------------------------------------------------

def _field_to_pg(field) -> tuple[str, Any, bool, bool]:
    """将 Tortoise Field 映射为 PostgreSQL 类型信息。

    Returns:
        (pg_type, default_value, nullable, inferred)

        inferred=True  表示 default 是本工具按类型推断的，非用户显式设置。
                       推断值仅用于 NOT NULL 列的 DEFAULT 填充，
                       nullable 列不写入推断默认值，避免污染数据库状态。
        inferred=False 表示用户显式设置了 default，nullable 列也会同步。
    """
    cls_name = type(field).__name__
    pg_base  = FIELD_TYPE_MAP.get(cls_name, "text")

    # CharEnumFieldInstance 是 CharField 子类，映射为 character varying
    # （FIELD_TYPE_MAP 中键名 "CharEnumField" 与实际类名不一致）
    is_char_enum = isinstance(field, CharEnumFieldInstance)

    # Precision types
    if cls_name == "CharField" or is_char_enum:
        max_len = getattr(field, "max_length", 255)
        pg_type = f"character varying({max_len})"
    elif cls_name == "DecimalField":
        pg_type = (
            f"numeric("
            f"{getattr(field, 'max_digits', 18)},"
            f"{getattr(field, 'decimal_places', 6)})"
        )
    else:
        pg_type = pg_base

    # Tortoise-ORM Field 没有 has_default 属性。
    # field.default 未设置时值为 None，与 default=None 无法区分，
    # 但本项目模型不使用 default=None（nullable 列用 null=True 代替），
    # 因此 field.default is not None 可安全替代 has_default。
    has_default = field.default is not None
    raw_default = getattr(field, "default", None)

    if has_default:
        # 用户显式设置了 default
        if isinstance(raw_default, Enum):
            default: Any = raw_default.value
        else:
            default = raw_default
        inferred = False
    else:
        # 未设置 default，按类型推断（仅用于 NOT NULL 列的 DEFAULT 填充）
        base = pg_base.split("(")[0].strip()
        if base in NUMERIC_TYPES:
            default = 0
        elif base in STRING_TYPES:
            default = ""
        elif pg_type.endswith("[]"):
            default = "{}"
        else:
            default = None   # jsonb / uuid 等不推断
        inferred = True

    nullable = getattr(field, "null", False)
    return pg_type, default, nullable, inferred


# ---------------------------------------------------------------------------
# Index collection
# ---------------------------------------------------------------------------

def _collect_indexes(model_cls) -> list[tuple[tuple[str, ...], bool]]:
    """
    Gather all index intent from a model into [(cols_tuple, is_unique)].

    Sources:
      1. Meta.indexes         (tuple or Index object) -> regular
      2. Meta.unique_together (tuple)                 -> unique
      3. field(index=True)                            -> regular single-col
      4. field(unique=True)                           -> unique single-col

    过滤规则：
      - 包含主键列的索引条目直接忽略（主键已自带索引）。
      - 同一列集合：unique 优先，regular 被 unique 覆盖时发出警告。

    注意：Meta.indexes / unique_together 中的字段名均为属性名，
    需通过 fields_db_projection 转换为 db 列名。
    转换失败（字段名不在 projection 中）时发出警告并跳过该条目。
    """
    meta   = model_cls._meta
    pk_col = meta.db_pk_column
    seen: dict[tuple[str, ...], bool] = {}   # db_cols -> is_unique

    # 属性名 -> db 列名的完整映射（含 FK source_field 情况）
    attr_to_db: dict[str, str] = dict(meta.fields_db_projection)

    def _resolve_cols(
        field_names: tuple[str, ...], source: str,
    ) -> tuple[str, ...] | None:
        """
        将字段属性名序列转换为 db 列名序列。
        任何一个名字解析失败则返回 None（并记录警告）。
        """
        db_cols: list[str] = []
        for name in field_names:
            db_col = attr_to_db.get(name)
            if db_col is None:
                logger.warning(
                    "[%s] %s 中字段 %r 不在 fields_db_projection，"
                    "无法解析为 db 列名，忽略整个索引条目（fields=%s）",
                    model_cls.__name__, source, name, field_names,
                )
                return None
            db_cols.append(db_col)
        return tuple(db_cols)

    def _add(cols: tuple[str, ...], unique: bool, source: str) -> None:
        if not cols:
            return

        # 兜底：Meta.indexes / unique_together 中手动包含主键列时拦截并警告
        if any(c == pk_col for c in cols):
            logger.warning(
                "[%s] %s 包含主键列 %r，主键已自带索引，忽略该条目（cols=%s）",
                model_cls.__name__, source, pk_col, cols,
            )
            return

        # ── 过滤含主键列的条目 ──────────────────────────────────────
        if any(c == pk_col for c in cols):
            logger.warning(
                "[%s] %s 包含主键列 %r，主键已自带索引，忽略该条目（cols=%s）",
                model_cls.__name__, source, pk_col, cols,
            )
            return

        # ── unique 与 regular 冲突检测 ──────────────────────────────
        existing = seen.get(cols)
        if existing is not None:
            if existing and not unique:
                # 已登记为 unique，现在又来一个 regular —— 忽略 regular
                logger.warning(
                    "[%s] %s cols=%s 已登记为 UNIQUE 索引，"
                    "重复的普通索引请求被忽略",
                    model_cls.__name__, source, cols,
                )
                return
            if not existing and unique:
                # 已登记为 regular，现在升级为 unique —— 升级并警告
                logger.warning(
                    "[%s] %s cols=%s 已登记为普通索引，"
                    "发现更强的 UNIQUE 约束，升级为 UNIQUE 索引",
                    model_cls.__name__, source, cols,
                )
                seen[cols] = True
                return
            # 完全重复（same uniqueness），静默忽略
            return

        seen[cols] = unique

    # Source 1: Meta.indexes
    for entry in (getattr(meta, "indexes", None) or []):
        if isinstance(entry, TortoiseIndex):
            db_cols = _resolve_cols(tuple(entry.fields), "Meta.indexes")
            if db_cols is not None:
                _add(db_cols, False, "Meta.indexes")
        elif isinstance(entry, (tuple, list)):
            db_cols = _resolve_cols(tuple(entry), "Meta.indexes")
            if db_cols is not None:
                _add(db_cols, False, "Meta.indexes")

    # Source 2: Meta.unique_together
    for entry in (getattr(meta, "unique_together", None) or []):
        if isinstance(entry, (tuple, list)):
            db_cols = _resolve_cols(tuple(entry), "Meta.unique_together")
            if db_cols is not None:
                _add(db_cols, True, "Meta.unique_together")

    # Sources 3 & 4: field-level flags
    # fields_db_projection 直接给出 db 列名，无需转换
    for field_attr, db_col in meta.fields_db_projection.items():
        field_obj = meta.fields_map.get(field_attr)
        if field_obj is None:
            continue

        # pk=True 的字段 unique/index 均由主键约束保证，跳过避免误报
        if getattr(field_obj, "pk", False):
            continue

        # Tortoise 1.1.7 将 index=True 存储为 field.index，
        # 没有 db_index 属性，同时兼容两个属性名以防版本差异。
        is_indexed = (
            getattr(field_obj, "db_index", False)
            or getattr(field_obj, "index", False)
        )
        if is_indexed:
            _add((db_col,), False, f"field({field_attr}, index=True)")

        if getattr(field_obj, "unique", False):
            _add((db_col,), True, f"field({field_attr}, unique=True)")

    return list(seen.items())


# ---------------------------------------------------------------------------
# FK warning — 本工具不处理外键约束，但应告知开发者
# ---------------------------------------------------------------------------

def _warn_fk(model_cls, debug: bool) -> None:
    """如果模型定义了 ForeignKeyField，输出警告日志。"""
    # Tortoise 1.1.7: meta.fk_fields 是 set[str]（字段属性名集合），不是对象列表
    fk_fields: set[str] = getattr(model_cls._meta, "fk_fields", set())
    if fk_fields:
        msg = (
            f"[{model_cls.__name__}] 定义了 ForeignKeyField: {sorted(fk_fields)}。"
            "本工具不处理外键约束，业务层需自行维护引用完整性。"
        )
        if debug:
            logger.warning(msg)
        else:
            logger.info(msg)


# ---------------------------------------------------------------------------
# Constraint index introspection helper
# ---------------------------------------------------------------------------

async def _fetch_constraint_indexes(conn, schema: str, table: str) -> set[str]:
    """
    查询 pg_constraint，返回由 UNIQUE / EXCLUDE 约束支撑的索引名集合。

    这类索引在 pg_indexes 中可见，但不能用 DROP INDEX 删除，
    必须用 ALTER TABLE ... DROP CONSTRAINT。

    注意：
    - PRIMARY KEY 约束已在调用方通过 indexname != '{table}_pkey' 过滤，
      此处仍用 contype IN ('u', 'x') 做精确过滤，避免 PK 被误操作。
    - conindid > 0 确保只返回真正有支撑索引的约束行。
    """
    _, rows = await conn.execute_query(
        """
        SELECT conname
        FROM   pg_constraint
        WHERE  conrelid = ($1 || '.' || $2)::regclass
          AND  contype  IN ('u', 'x')
          AND  conindid  > 0
        """,
        [schema, table],
    )
    return {r["conname"] for r in rows}


# ---------------------------------------------------------------------------
# SchemaSync
# ---------------------------------------------------------------------------

class SchemaSync:
    """
    检查 live Postgres 并应用最小 DDL 以匹配 Tortoise 模型定义。

    典型调用:
        await Tortoise.init(config=TORTOISE_ORM)
        await SchemaSync.sync(debug=True)

    清理遗留约束:
        await SchemaSync.migrate_from_aerich(debug=True)
        await SchemaSync.sync(force=True, debug=True)
    """

    @classmethod
    async def sync(
        cls,
        *,
        drop_not_exist_col: bool = False,
        schema: str = "public",
        debug: bool = False,
        only: list[str] | None = None,
        force: bool = False,
    ) -> None:
        """
        同步所有已注册的 Tortoise 模型。

        Args:
            drop_not_exist_col: 删除数据库中存在但模型定义中不存在的列。
                                生产环境请谨慎使用。
            schema:             Postgres schema 名称（默认 'public'）。
            debug:              True 时日志输出每条 DDL 语句。
            only:               可选的模型类名列表，限制只同步这些模型。
                                None（默认）同步全部。
            force:              强制模式：
                                  - 类型不兼容时 DROP+ADD 重建列；
                                  - 清理所有不在模型定义中的非系统索引
                                    （包括遗留的 _key 约束，
                                     普通模式只管理 _idx/_uidx 后缀的索引）。
        """
        conn    = Tortoise.get_connection("default")
        db_name = await cls._db_name(conn)

        synced = 0
        for _app, app_models in Tortoise.apps.items():
            for model_name, model_cls in app_models.items():
                if only and model_name not in only:
                    continue
                if getattr(model_cls._meta, "abstract", False):
                    continue
                await cls._sync_model(
                    conn, model_cls,
                    db_name=db_name,
                    schema=schema,
                    drop_not_exist_col=drop_not_exist_col,
                    debug=debug,
                    force=force,
                )
                synced += 1

        if debug:
            logger.info("[sync] 完成，共处理 %d 个模型", synced)

    @classmethod
    async def create_schema(cls, schema: str) -> None:
        """创建 schema（如果不存在）。"""
        conn = Tortoise.get_connection("default")
        await conn.execute_script(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    @classmethod
    async def migrate_from_aerich(
        cls,
        *,
        schema: str = "public",
        debug: bool = False,
    ) -> None:
        """
        清理遗留的 UNIQUE 约束（PostgreSQL 自动命名为 _key 后缀）。

        generate_schema 使用 ALTER TABLE ... ADD CONSTRAINT 创建唯一约束，
        而本工具改用 CREATE UNIQUE INDEX（_uidx 后缀）。
        两套机制并存时会出现重复保障，且 force 模式下直接 DROP INDEX 会报错。

        本方法删除所有 _key 后缀的 UNIQUE 约束，之后由 sync(force=True) 按
        模型定义重建为 _uidx 索引。

        幂等：重复调用无副作用（IF EXISTS 保护）。

        典型用法:
            await SchemaSync.migrate_from_aerich(debug=True)
            await SchemaSync.sync(force=True, debug=True)
        """
        conn = Tortoise.get_connection("default")

        # 查找所有 _key 后缀的 UNIQUE 约束（排除系统表）
        _, rows = await conn.execute_query(
            """
            SELECT c.conname,
                   c.conrelid::regclass AS tbl
            FROM   pg_constraint c
            JOIN   pg_class       r ON r.oid = c.conrelid
            JOIN   pg_namespace   n ON n.oid = r.relnamespace
            WHERE  c.contype    = 'u'
              AND  c.conname   LIKE '%_key'
              AND  n.nspname    = $1
            ORDER  BY tbl, c.conname
            """,
            [schema],
        )

        if not rows:
            if debug:
                logger.info("[migrate_from_aerich] 未发现遗留约束，无需清理")
            return

        for r in rows:
            sql = f'ALTER TABLE {r["tbl"]} DROP CONSTRAINT IF EXISTS "{r["conname"]}"'
            if debug:
                logger.info("[migrate_from_aerich] %s", sql)
            await conn.execute_script(sql)

        logger.info(
            "[migrate_from_aerich] 已清理 %d 个遗留约束，"
            "请执行 sync(force=True) 重建索引",
            len(rows),
        )

    # ------------------------------------------------------------------ #
    #  Per-model orchestration
    # ------------------------------------------------------------------ #

    @classmethod
    async def _sync_model(
        cls, conn, model_cls, *, db_name, schema,
        drop_not_exist_col, debug, force,
    ) -> None:
        table  = model_cls._meta.db_table
        cur    = f"{schema}.{table}"
        pk_col = model_cls._meta.db_pk_column or "id"

        if debug:
            logger.info("[sync] %s (%s)", model_cls.__name__, table)

        # FK warning（本工具不处理，但告知开发者）
        _warn_fk(model_cls, debug)

        # 获取模型期望的列定义
        fields_def = cls._resolve_fields(model_cls)

        # 检查表是否存在
        if not await cls._table_exists(conn, db_name, schema, table):
            await cls._create_table(conn, cur, pk_col, fields_def, debug)
        else:
            db_cols = await cls._fetch_columns(conn, db_name, schema, table)
            await cls._sync_columns(
                conn, cur, pk_col, fields_def, db_cols,
                model_cls=model_cls,
                drop_not_exist_col=drop_not_exist_col,
                debug=debug,
                force=force,
            )

        # 索引同步
        await cls._sync_indexes(conn, model_cls, schema, cur, table, debug, force)

        if debug:
            logger.info("[sync] done  %s", table)

    # ------------------------------------------------------------------ #
    #  Field resolution — 使用 fields_db_projection 确保只处理真实列
    # ------------------------------------------------------------------ #

    @classmethod
    def _resolve_fields(cls, model_cls) -> dict[str, tuple[str, Any, bool, bool]]:
        """
        Return {db_column_name: (pg_type, default, nullable, inferred)}
        for every real DB column this model owns.
        """
        meta   = model_cls._meta
        result = {}

        for field_attr, db_col in meta.fields_db_projection.items():
            field_obj = meta.fields_map.get(field_attr)
            if field_obj is None:
                continue
            result[db_col] = _field_to_pg(field_obj)

        return result

    # ------------------------------------------------------------------ #
    #  CREATE TABLE
    # ------------------------------------------------------------------ #

    @classmethod
    async def _create_table(
        cls, conn, cur: str, pk_col: str,
        fields_def: dict[str, tuple[str, Any, bool, bool]], debug: bool,
    ) -> None:
        col_defs: list[str] = []

        for col, (pg_type, default, nullable, inferred) in fields_def.items():
            if col == pk_col:
                col_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
                continue

            line = f'"{col}" {pg_type}'
            if nullable:
                # nullable 列：只有用户显式设置了 default 才写入
                if not inferred and default is not None:
                    line += f" DEFAULT {cls._fmt(default)}"
            else:
                line += " NOT NULL"
                if default is not None:
                    line += f" DEFAULT {cls._fmt(default)}"

            col_defs.append(line)

        ddl = f"CREATE TABLE IF NOT EXISTS {cur} ({', '.join(col_defs)})"
        if debug:
            logger.info(ddl)
        await conn.execute_script(ddl)

    # ------------------------------------------------------------------ #
    #  Column sync
    # ------------------------------------------------------------------ #

    @classmethod
    async def _sync_columns(
        cls, conn, cur: str, pk_col: str,
        fields_def: dict[str, tuple[str, Any, bool, bool]],
        db_cols: dict[str, dict],
        *,
        model_cls,
        drop_not_exist_col: bool,
        debug: bool,
        force: bool = False,
    ) -> None:
        # 列重命名支持 (Meta.old_names: {new_col_name: old_col_name})
        old_names: dict[str, str] = getattr(model_cls._meta, "old_names", {})

        # 先处理重命名，再处理增改
        for col, (pg_type, default, nullable, inferred) in fields_def.items():
            # 主键列类型同步暂不开启（同下方逐列处理注释）
            if col == pk_col:
                continue

            old_col = old_names.get(col)
            if old_col and old_col in db_cols and col not in db_cols:
                sql = f'ALTER TABLE {cur} RENAME COLUMN "{old_col}" TO "{col}"'
                if debug:
                    logger.info(sql)
                await conn.execute_script(sql)
                db_cols[col] = db_cols.pop(old_col)  # 继承元信息

        # 逐列处理: ADD / ALTER TYPE / SET DEFAULT / SET NOT NULL
        for col, (pg_type, default, nullable, inferred) in fields_def.items():
            # 主键列类型同步暂不开启（遗留 character→varchar 无需额外处理，
            # 新建表已由 _create_table 保证正确，未来可移除此注释启用）
            if col == pk_col:
                continue

            # ── ADD missing column ────────────────────────────────────
            if col not in db_cols:
                sql = f'ALTER TABLE {cur} ADD COLUMN "{col}" {pg_type}'
                if nullable:
                    # nullable 列：只有用户显式设置了 default 才写入
                    if not inferred and default is not None:
                        sql += f" DEFAULT {cls._fmt(default)}"
                else:
                    sql += " NOT NULL"
                    if default is not None:
                        sql += f" DEFAULT {cls._fmt(default)}"
                if debug:
                    logger.info(sql)
                await conn.execute_script(sql)
                continue

            db_col = db_cols[col]

            # ── TYPE change — 先试，失败再 force 重建 ─────────────────
            if not cls._types_match(db_col, pg_type):
                sql = f'ALTER TABLE {cur} ALTER COLUMN "{col}" TYPE {pg_type}'
                if debug:
                    logger.info(sql)
                try:
                    await conn.execute_script(sql)
                except Exception as exc:
                    if force:
                        logger.info(
                            "[force] %s.%s 类型不兼容 (%s → %s)，DROP+ADD 重建",
                            cur, col, db_col["data_type"], pg_type,
                        )
                        await conn.execute_script(
                            f'ALTER TABLE {cur} DROP COLUMN "{col}"'
                        )
                        add_sql = f'ALTER TABLE {cur} ADD COLUMN "{col}" {pg_type}'
                        if nullable:
                            if not inferred and default is not None:
                                add_sql += f" DEFAULT {cls._fmt(default)}"
                        else:
                            add_sql += " NOT NULL"
                            if default is not None:
                                add_sql += f" DEFAULT {cls._fmt(default)}"
                        if debug:
                            logger.info(add_sql)
                        await conn.execute_script(add_sql)
                        continue
                    else:
                        logger.error(
                            "无法修改 %s.%s 类型为 %s: %s\n"
                            "如需强制重建，请使用 --force 参数。",
                            cur, col, pg_type, exc,
                        )
                        continue

            # ── DEFAULT change ────────────────────────────────────────
            # nullable 列且 default 是推断值：跳过，不污染数据库现有状态。
            # 其余情况（NOT NULL 列 / 用户显式设置的 nullable 列）正常同步。
            db_default     = db_col.get("column_default")
            target_default = cls._fmt(default) if default is not None else None

            if target_default is not None and not (nullable and inferred):
                normalized = cls._normalize_default(db_default)
                if normalized != target_default:
                    sql = (
                        f'ALTER TABLE {cur} '
                        f'ALTER COLUMN "{col}" SET DEFAULT {target_default}'
                    )
                    if debug:
                        logger.info(sql)
                    await conn.execute_script(sql)

            # ── NOT NULL enforcement ──────────────────────────────────
            # 注意：若列中已有 NULL 值，直接 SET NOT NULL 会报错。
            # 先将 NULL 值填充为默认值，再施加约束。
            if not nullable and db_col["is_nullable"] == "YES":
                if default is not None:
                    fill_sql = (
                        f'UPDATE {cur} '
                        f'SET "{col}" = {cls._fmt(default)} '
                        f'WHERE "{col}" IS NULL'
                    )
                    if debug:
                        logger.info(fill_sql)
                    await conn.execute_script(fill_sql)

                sql = f'ALTER TABLE {cur} ALTER COLUMN "{col}" SET NOT NULL'
                if debug:
                    logger.info(sql)
                await conn.execute_script(sql)

        # ── DROP columns absent from model ───────────────────────────
        if drop_not_exist_col:
            for db_col_name in list(db_cols):
                if db_col_name not in fields_def and db_col_name != pk_col:
                    db_col = db_cols[db_col_name]
                    # 保护计算列，防止误删
                    if db_col.get("is_generated") == "ALWAYS":
                        if debug:
                            logger.info("跳过计算列: %s.%s", cur, db_col_name)
                        continue
                    sql = (
                        f'ALTER TABLE {cur} '
                        f'DROP COLUMN IF EXISTS "{db_col_name}"'
                    )
                    if debug:
                        logger.info(sql)
                    await conn.execute_script(sql)

    # ------------------------------------------------------------------ #
    #  Index sync
    # ------------------------------------------------------------------ #

    @classmethod
    async def _sync_indexes(
        cls, conn, model_cls, schema: str, cur: str, table: str,
        debug: bool, force: bool = False,
    ) -> None:
        # Desired: name -> (cols, unique)
        desired: dict[str, tuple[tuple[str, ...], bool]] = {
            _make_index_name(table, cols, unique): (cols, unique)
            for cols, unique in _collect_indexes(model_cls)
        }

        # ── 查询由 UNIQUE / EXCLUDE 约束支撑的索引 ───────────────────
        # 这类索引在 pg_indexes 中可见，但必须通过 DROP CONSTRAINT 删除。
        constraint_indexes = await _fetch_constraint_indexes(conn, schema, table)

        # ── 查询现有索引（排除主键）──────────────────────────────────
        _, rows = await conn.execute_query(
            """
            SELECT indexname, indexdef
            FROM   pg_indexes
            WHERE  tablename  = $1
              AND  schemaname = $2
              AND  indexname  != $3
            """,
            [table, schema, f"{table}_pkey"],
        )

        if force:
            # force 模式：清理所有非系统索引
            existing: dict[str, str] = {
                r["indexname"]: r["indexdef"]
                for r in rows
                if not r["indexname"].startswith("pg_")
            }
        else:
            # 普通模式：只管理本工具创建的索引（_idx / _uidx 后缀）
            existing = {
                r["indexname"]: r["indexdef"]
                for r in rows
                if _is_managed(r["indexname"])
            }

        # ── DROP 已废弃的索引 ─────────────────────────────────────────
        for iname in existing:
            if iname not in desired:
                if iname in constraint_indexes:
                    # 约束支撑的索引：必须 DROP CONSTRAINT
                    sql = f'ALTER TABLE {cur} DROP CONSTRAINT IF EXISTS "{iname}"'
                else:
                    sql = f"DROP INDEX IF EXISTS {schema}.{iname}"
                if debug:
                    logger.info(sql)
                await conn.execute_script(sql)

        # ── CREATE 缺失的索引 ─────────────────────────────────────────
        for iname, (cols, unique) in desired.items():
            if iname in existing:
                continue
            col_list = ", ".join(f'"{c}"' for c in cols)
            kind     = "UNIQUE INDEX" if unique else "INDEX"
            sql      = f"CREATE {kind} {iname} ON {cur} ({col_list})"
            if debug:
                logger.info(sql)
            await conn.execute_script(sql)

    # ------------------------------------------------------------------ #
    #  DB introspection helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    async def _db_name(conn) -> str:
        _, rows = await conn.execute_query("SELECT current_database()")
        return rows[0]["current_database"]

    @staticmethod
    async def _table_exists(conn, db_name: str, schema: str, table: str) -> bool:
        _, rows = await conn.execute_query(
            """
            SELECT 1 FROM information_schema.tables
            WHERE  table_catalog = $1
              AND  table_schema  = $2
              AND  table_name    = $3
            """,
            [db_name, schema, table],
        )
        return bool(rows)

    @staticmethod
    async def _fetch_columns(
        conn, db_name: str, schema: str, table: str,
    ) -> dict[str, dict]:
        _, rows = await conn.execute_query(
            """
            SELECT column_name,
                   data_type,
                   column_default,
                   character_maximum_length,
                   numeric_precision,
                   numeric_scale,
                   is_nullable,
                   is_generated
            FROM   information_schema.columns
            WHERE  table_catalog = $1
              AND  table_schema  = $2
              AND  table_name    = $3
            """,
            [db_name, schema, table],
        )
        return {r["column_name"]: dict(r) for r in rows}

    # ------------------------------------------------------------------ #
    #  Type comparison
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_base(pg_type: str) -> str:
        return re.split(r"[(\[]", pg_type)[0].strip().lower()

    @classmethod
    def _types_match(cls, db_col: dict, target: str) -> bool:
        db_type = db_col["data_type"].lower()
        base    = cls._parse_base(target)

        if base not in TYPES_WITH_BRACKETS:
            return db_type == base

        # character(定长) 与 character varying(变长) 是不同类型，不可视为兼容
        # （旧工具用 character，schema_sync 用 character varying）
        # text 与 character varying 可自动转换，无需拦截
        if db_type == "character" and base == "character varying":
            return False

        if base == "character varying":
            m = re.search(r"\((\d+)\)", target)
            return db_col.get("character_maximum_length") == (
                int(m.group(1)) if m else None
            )

        if base == "numeric":
            m = re.search(r"\((\d+),(\d+)\)", target)
            if not m:
                return db_type == "numeric"
            return (
                db_col.get("numeric_precision") == int(m.group(1))
                and db_col.get("numeric_scale") == int(m.group(2))
            )

        return False

    # ------------------------------------------------------------------ #
    #  Default value normalization & formatting
    # ------------------------------------------------------------------ #

    @staticmethod
    def _normalize_default(pg_default: str | None) -> str | None:
        """
        将 PostgreSQL 返回的 column_default 标准化为与 _fmt() 可比较的格式。

        PostgreSQL 返回示例:
            NULL, null
            ''::character varying
            0, 0::integer
            true, false
            'some value'::text
        """
        if pg_default is None:
            return None
        pg = pg_default.strip()

        if pg.lower() == "null":
            return None
        if pg.lower() == "true":
            return "TRUE"
        if pg.lower() == "false":
            return "FALSE"
        if pg == "''" or pg.startswith("''::"):
            return "''"

        # 先剥离类型转换后缀（含数值类型，如 0::integer），再做后续判断
        if "::" in pg:
            pg = pg.split("::")[0]

        if pg.startswith("'") and pg.endswith("'"):
            return pg

        # Numeric — normalize to plain int/float string
        try:
            return str(int(pg))
        except ValueError:
            try:
                val = float(pg)
                if val == int(val):
                    return str(int(val))
                return str(val)
            except ValueError:
                return pg

    @staticmethod
    def _fmt(value: Any) -> str:
        """将 Python 值格式化为 SQL DEFAULT 字面值。"""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        return "'{}'".format(str(value).replace("'", "''"))
