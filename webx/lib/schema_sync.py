"""
webx/lib/schema_sync.py  —  NeoPG-style runtime schema sync for Tortoise-ORM
-----------------------------------------------------------------------------
Design philosophy: 模型定义即权威,information_schema 为真相,差异最小化为目标。
手动执行一次完成同步,不做运行时锁保护。

Index naming (deterministic, no randomness):
    {table}_{cols_in_order}_idx     regular index
    {table}_{cols_in_order}_uidx    unique index

列顺序**按模型声明的原样**参与命名，不排序——列顺序是复合索引语义的一部分，
排序会让 (a,b) 与 (b,a) 撞名并静默丢掉一条（详见 _make_index_name）。

两种同步强度：
    普通模式  只管理 _idx / _uidx 后缀的索引，外部索引一概不碰。
    force     **硬同步**：模型定义即全部真相，凡不在模型定义中的索引一律清除
              （其它工具遗留的野索引、手工建的临时索引、旧命名规则的历史索引）。
              唯二豁免是主键与 pg_ 开头的系统索引。

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

    # 硬同步：清掉一切不在模型定义中的索引（含其它工具遗留的野索引与旧命名）
    await SchemaSync.sync(force=True, debug=True)
"""

from __future__ import annotations

import logging
import re
from enum import Enum
from typing import Any, NamedTuple

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

#: 主键索引的 PostgreSQL 默认命名后缀。与 pg_constraint 的 contype='p' 一起构成
#: **双重保护**：任一命中即视为主键、永不删除（见 _sync_indexes 的 protected）。
_PKEY = "_pkey"

#: PostgreSQL 标识符上限（NAMEDATALEN-1），单位是**字节**不是字符。
#: 超长不报错、**静默截断**——那会让 pg_indexes 回读的名字与 desired 的 key
#: 永远对不上，每次同步都重复 CREATE 并报 already exists。故超长直接拒绝。
_MAX_IDENTIFIER_BYTES = 63


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------

class IndexSpec(NamedTuple):
    """一条索引的**完整**意图。

    存在的理由：四个信息源里只有 ``Meta.indexes`` 能携带列以外的信息
    （索引方法、部分索引条件、表达式、自定义名）。早期版本把它们统一压成
    ``(cols, unique)`` 二元组去做去重，于是 ``GinIndex(fields=("files_json",))``
    被**静默降级**成普通 btree 索引——不报错、不告警，``\\d`` 里看着还在，
    只有 EXPLAIN 才会暴露它服务不了 JSONB 的 ``@>`` 查询。本结构负责把信息
    完整带到 DDL 生成处。

    字段：
        cols:       列名元组；表达式索引里是 ``"(表达式SQL)"``（自带括号）
        unique:     是否唯一索引
        index_type: 索引方法，``""`` = btree（PG 默认）；GIN / BRIN / GIST / …
        extra:      尾部子句，如 ``" WHERE is_active = true"``（含前导空格）
        name:       自定义索引名；空则按 ``_make_index_name`` 的规则派生
    """

    cols: tuple[str, ...]
    unique: bool = False
    index_type: str = ""
    extra: str = ""
    name: str = ""

    @property
    def dedup_key(self) -> tuple[tuple[str, ...], str, str]:
        """去重键 = 列 + 方法 + 条件。

        **不含 unique**：同一列集合的 unique 与 regular 是需要相互覆盖的冲突
        （见 _add）。**含 index_type / extra**：同一列上的 btree 与 GIN 是两条
        各自有用的索引，同列不同条件的部分索引亦然，它们不该互相顶掉。
        """
        return (self.cols, self.index_type, self.extra)


def _is_plain_column(col: str) -> bool:
    """该项是否为可安全嵌进索引名的裸列名（而非表达式）。"""
    return col.replace("_", "").isalnum()


def _make_index_name(table: str, cols: tuple[str, ...], unique: bool) -> str:
    """Deterministic name: {table}_{cols_in_order}_{_idx|_uidx}

    ⚠️ **刻意不排序**。列顺序是复合索引语义的一部分（最左前缀原则）：
    ``(user_id, created_at)`` 服务「按用户过滤再按时间排序」，
    ``(created_at, user_id)`` 服务「按时间范围扫描」，二者不可互相替代、
    同时声明是合理需求。若按排序后的列名生成名字，这两条会撞成同一个名字，
    在 desired 字典里后者覆盖前者、**静默丢掉一条且不产生任何告警**。

    附带效果：名字里的列顺序 == CREATE INDEX 的列顺序，
    从 ``\\d table`` 看到的索引名可以直接反推它能服务哪些查询。
    """
    return f"{table}_{'_'.join(cols)}{'_uidx' if unique else '_idx'}"


def _resolve_index_name(table: str, spec: IndexSpec) -> str | None:
    """定出 spec 的最终索引名；无法确定性命名时返回 ``None``（调用方跳过并告警）。

    优先用显式 ``name``。否则按 ``{table}_{cols}[_{type}]_{idx|uidx}`` 派生，
    索引方法进名字是必须的——否则同一列上的 btree 与 GIN 会撞名。

    **两种情况强制要求显式 name**，因为它们无法确定性地映射成一个稳定的标识符：

      - 表达式索引：``"(LOWER(email))"`` 这类串带括号与运算符，塞进索引名既非法
        又不可逆；
      - 部分索引（``extra`` 非空）：条件是任意 SQL 片段，哈希进名字会让人看不懂，
        原样拼接则非法。

    宁可要求显式命名，也不生成一个「看起来能用、实际对不上」的名字——本模块判定
    索引存在与否**只比名字**（见 _sync_indexes），名字一旦不确定，DROP/CREATE 的
    判定就全乱了。
    """
    if spec.name:
        name = spec.name
    elif spec.extra or not all(_is_plain_column(c) for c in spec.cols):
        return None
    else:
        type_seg = f"_{spec.index_type.lower()}" if spec.index_type else ""
        suffix = _UIDX if spec.unique else _IDX
        name = f"{table}_{'_'.join(spec.cols)}{type_seg}{suffix}"

    # ── 名字本身的两条硬约束 ──────────────────────────────────────────
    # ① 不得含双引号：DDL 里索引名用 "…" 引用，名字内再出现 " 会截断引用
    #    （PostgreSQL 的转义写法是 ""，本模块不做转义——索引名带引号本就不该出现，
    #     拒绝比默默转义更容易发现问题）。
    # ② 不得超 63 字节：超长 PostgreSQL 静默截断，回读名字与 desired 的 key 对不上，
    #    每次同步都重复 CREATE。长表名 + 多列复合索引真的会撞到这条线。
    if '"' in name or len(name.encode("utf-8")) > _MAX_IDENTIFIER_BYTES:
        return None
    return name


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
    raw_default = getattr(field, "default", None)

    # ── 可调用的 default 视同「未设置」──────────────────────────────────
    # gen_id / list / dict 这类由 ORM 在**每次插入时**求值，库层没有等价物。
    # 若当作普通值交给 _fmt，会得到 DEFAULT '<function gen_id at 0x7f1f30c89080>'：
    #   ① 任何绕过 ORM 的写入（INSERT 不带该列 / COPY 导入）都会拿到这个字符串；
    #   ② 0x7f1f… 是内存地址，进程重启就变 → 每次同步都判定 default 变了、
    #      反复 SET DEFAULT，且每次写进去的值都不同。
    # 排除后落入下面的类型推断分支（NOT NULL 列仍能拿到合理的填充值）。
    # 当前项目的 callable default 全在主键上（主键在建表与列同步两条路径上均被
    # 跳过），本判断不产生任何 DDL 差异；它防的是 JSONField(default=dict) 这类
    # **官方推荐写法**被用在非主键列上。
    has_default = raw_default is not None and not callable(raw_default)

    if has_default:
        # 用户显式设置了标量 default
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

def _collect_indexes(model_cls) -> list[IndexSpec]:
    """
    Gather all index intent from a model into [IndexSpec].

    Sources:
      1. Meta.indexes         (tuple or Index object) -> regular
      2. Meta.unique_together (tuple)                 -> unique
      3. field(index=True)                            -> regular single-col
      4. field(unique=True)                           -> unique single-col

    只有 Source 1 的 Index 对象能携带列以外的信息（索引方法 / 部分索引条件 /
    表达式 / 自定义名），这些经 IndexSpec 完整带到 DDL 生成处；其余三个来源
    产出的都是最朴素的 btree 裸列索引。

    过滤规则：
      - 包含主键列的索引条目直接忽略（主键已自带索引）。
      - 同一 (列, 方法, 条件)：unique 优先，regular 被 unique 覆盖时发出警告。
        —— 同列的 btree 与 GIN **不算冲突**，二者各自有用（见 IndexSpec.dedup_key）。

    注意：Meta.indexes / unique_together 中的字段名均为属性名，
    需通过 fields_db_projection 转换为 db 列名。
    转换失败（字段名不在 projection 中）时发出警告并跳过该条目。
    """
    meta   = model_cls._meta
    pk_col = meta.db_pk_column
    seen: dict[tuple[tuple[str, ...], str, str], IndexSpec] = {}

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

    def _add(spec: IndexSpec, source: str) -> None:
        if not spec.cols:
            return

        # ── 过滤含主键列的条目（主键已自带索引）────────────────────
        if any(c == pk_col for c in spec.cols):
            logger.warning(
                "[%s] %s 包含主键列 %r，主键已自带索引，忽略该条目（cols=%s）",
                model_cls.__name__, source, pk_col, spec.cols,
            )
            return

        # ── unique 与 regular 冲突检测（同 列+方法+条件 才算冲突）──
        key = spec.dedup_key
        existing = seen.get(key)
        if existing is not None:
            if existing.unique and not spec.unique:
                # 已登记为 unique，现在又来一个 regular —— 忽略 regular
                logger.warning(
                    "[%s] %s cols=%s 已登记为 UNIQUE 索引，"
                    "重复的普通索引请求被忽略",
                    model_cls.__name__, source, spec.cols,
                )
                return
            if not existing.unique and spec.unique:
                # 已登记为 regular，现在升级为 unique —— 升级并警告
                logger.warning(
                    "[%s] %s cols=%s 已登记为普通索引，"
                    "发现更强的 UNIQUE 约束，升级为 UNIQUE 索引",
                    model_cls.__name__, source, spec.cols,
                )
                seen[key] = existing._replace(unique=True)
                return
            # 完全重复（same uniqueness），静默忽略
            return

        seen[key] = spec

    # Source 1: Meta.indexes —— 唯一能携带方法 / 条件 / 表达式 / 自定义名的来源
    for entry in (getattr(meta, "indexes", None) or []):
        if isinstance(entry, TortoiseIndex):
            # fields 走属性名→列名转换；expressions 已是 "(SQL)" 串，原样带过去。
            # 二者在 Tortoise 里互斥（Index.__init__ 强制），故这里非此即彼。
            if entry.fields:
                db_cols = _resolve_cols(tuple(entry.fields), "Meta.indexes")
            else:
                db_cols = tuple(entry.field_names)
            if db_cols is not None:
                _add(
                    IndexSpec(
                        cols=db_cols,
                        unique=False,
                        index_type=getattr(type(entry), "INDEX_TYPE", "") or "",
                        extra=getattr(entry, "extra", "") or "",
                        name=getattr(entry, "name", "") or "",
                    ),
                    "Meta.indexes",
                )
        elif isinstance(entry, (tuple, list)):
            db_cols = _resolve_cols(tuple(entry), "Meta.indexes")
            if db_cols is not None:
                _add(IndexSpec(cols=db_cols), "Meta.indexes")

    # Source 2: Meta.unique_together
    for entry in (getattr(meta, "unique_together", None) or []):
        if isinstance(entry, (tuple, list)):
            db_cols = _resolve_cols(tuple(entry), "Meta.unique_together")
            if db_cols is not None:
                _add(IndexSpec(cols=db_cols, unique=True), "Meta.unique_together")

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
            _add(IndexSpec(cols=(db_col,)), f"field({field_attr}, index=True)")

        if getattr(field_obj, "unique", False):
            _add(
                IndexSpec(cols=(db_col,), unique=True),
                f"field({field_attr}, unique=True)",
            )

    return list(seen.values())


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

async def _fetch_constraint_indexes(conn, schema: str, table: str) -> dict[str, str]:
    """查询由约束支撑的索引，返回 ``{名字: contype}``。

    约束支撑的索引在 ``pg_indexes`` 中可见，但**不能用 DROP INDEX 删除**，
    必须走 ``ALTER TABLE ... DROP CONSTRAINT``。三类的处置不同：

      - ``p`` PRIMARY KEY —— **永不删除**，返回它只为让调用方识别并跳过
      - ``u`` UNIQUE / ``x`` EXCLUDE —— 不在模型定义中时用 DROP CONSTRAINT 清理

    **约束名即索引名**，这是 PostgreSQL 的保证而非巧合，故 conname 可直接当索引名用，
    无需再 JOIN pg_class 按 conindid 反查。依据（ALTER TABLE 文档，ADD
    table_constraint USING INDEX 一节）：

        "If a constraint name is provided then the index will be renamed to match
         the constraint name. Otherwise the constraint will be named the same as
         the index."

    即便拿一个既有索引去提升为约束，PostgreSQL 也会把索引重命名成约束名，
    二者不存在长期不一致的状态。

    ``conindid > 0`` 确保只返回真正有支撑索引的约束行（CHECK 等没有）。
    """
    _, rows = await conn.execute_query(
        """
        SELECT conname, contype
        FROM   pg_constraint
        WHERE  conrelid = ($1 || '.' || $2)::regclass
          AND  contype  IN ('p', 'u', 'x')
          AND  conindid  > 0
        """,
        [schema, table],
    )
    return {r["conname"]: r["contype"] for r in rows}


# ---------------------------------------------------------------------------
# SchemaSync
# ---------------------------------------------------------------------------

class SchemaSync:
    """
    检查 live Postgres 并应用最小 DDL 以匹配 Tortoise 模型定义。

    典型调用:
        await Tortoise.init(config=TORTOISE_ORM)
        await SchemaSync.sync(debug=True)

    硬同步（清掉一切不在模型定义中的索引）:
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
            force:              **硬同步**（模型定义即全部真相）：
                                  - 类型不兼容时 DROP+ADD 重建列（⚠️ 该列数据丢失）；
                                  - 清理所有不在模型定义中的索引，不看命名规则——
                                    含其它工具遗留的野索引、手工建的临时索引、
                                    旧命名规则的历史索引、以及由 UNIQUE/EXCLUDE
                                    约束支撑的索引（走 DROP CONSTRAINT）。
                                    唯二豁免：主键与 pg_ 开头的系统索引。
                                  ⚠️ 手工创建的特殊索引（如 JSONB 的 GIN 索引）
                                     无法在模型中声明，本模式下会被删除。
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
        # Desired: name -> IndexSpec
        # 不用字典推导：命名失败（表达式/部分索引未显式 name）要**告警并跳过**，
        # 而不是悄悄少建一条——本模块判定索引存在与否只比名字，静默跳过等于骗人。
        desired: dict[str, IndexSpec] = {}
        for spec in _collect_indexes(model_cls):
            iname = _resolve_index_name(table, spec)
            if iname is None:
                logger.warning(
                    "[%s] 索引 cols=%s type=%r extra=%r 无法确定性命名，已跳过。"
                    "表达式索引与部分索引请在 Index(..., name=\"…\") 中显式指定名字。",
                    model_cls.__name__, spec.cols, spec.index_type, spec.extra,
                )
                continue
            desired[iname] = spec

        # ── 约束支撑的索引：{名字: contype}（约束名即索引名，PG 保证）─────
        # 这类索引在 pg_indexes 中可见，但必须通过 DROP CONSTRAINT 删除。
        constraint_indexes = await _fetch_constraint_indexes(conn, schema, table)

        # ── 查询现有索引 ──────────────────────────────────────────────
        _, rows = await conn.execute_query(
            """
            SELECT indexname, indexdef
            FROM   pg_indexes
            WHERE  tablename  = $1
              AND  schemaname = $2
            """,
            [table, schema],
        )

        # ── 主键保护名单（任何模式下都不删）──────────────────────────
        # **双重判定，任一命中即保护**，两条判据相互独立：
        #   ① contype='p' —— 查 pg_constraint 得到的权威判据；
        #   ② 名字以 _pkey 结尾 —— PostgreSQL 的默认命名，纯字符串判断，
        #      连 pg_constraint 都不查（该表查询若因权限等原因返回空也照样兜住）。
        # 删掉主键索引会让表直接失去主键约束，代价远高于「多留一个名字像主键的索引」，
        # 故这里刻意取并集而非交集：宁可漏删，不可误删。
        protected = {n for n, ctype in constraint_indexes.items() if ctype == "p"}
        protected |= {r["indexname"] for r in rows if r["indexname"].endswith(_PKEY)}

        if force:
            # ── 硬同步：模型定义即全部真相 ────────────────────────────
            # 凡不在模型定义中的索引一律清除，不看命名规则——包括其它工具遗留的
            # 「野索引」、手工建的临时索引、旧命名规则的历史索引。
            # 唯二豁免：主键（protected，删了表就废了）与 pg_ 开头的系统索引。
            #
            # ⚠️ 手工创建的特殊索引（如 JSONB 的 GIN 索引）在本模式下**会被删除**。
            #    本工具只能表达裸列 btree 索引，无法在模型里声明它们；
            #    如有此类索引，要么别用 --force，要么先扩展本模块的索引表达能力。
            existing: dict[str, str] = {
                r["indexname"]: r["indexdef"]
                for r in rows
                if not r["indexname"].startswith("pg_") and r["indexname"] not in protected
            }
        else:
            # 普通模式：只管理本工具创建的索引（_idx / _uidx 后缀），
            # 外部索引一概不碰。主键天然不匹配这两个后缀，无需额外排除。
            existing = {
                r["indexname"]: r["indexdef"]
                for r in rows
                if _is_managed(r["indexname"]) and r["indexname"] not in protected
            }

        # ── DROP 已废弃的索引 ─────────────────────────────────────────
        for iname in existing:
            if iname not in desired:
                if iname in constraint_indexes:
                    # 约束支撑的索引：必须 DROP CONSTRAINT（约束名即索引名）
                    sql = f'ALTER TABLE {cur} DROP CONSTRAINT IF EXISTS "{iname}"'
                else:
                    sql = f'DROP INDEX IF EXISTS {schema}."{iname}"'
                if debug:
                    logger.info(sql)
                await conn.execute_script(sql)

        # ── CREATE 缺失的索引 ─────────────────────────────────────────
        for iname, spec in desired.items():
            if iname in existing:
                continue
            # 表达式项（field_names already yields "(SQL)"）原样输出，裸列名加引号
            col_list = ", ".join(
                f'"{c}"' if _is_plain_column(c) else c for c in spec.cols
            )
            kind  = "UNIQUE INDEX" if spec.unique else "INDEX"
            # 索引方法：**空 = 不写 USING**，由 PostgreSQL 用默认的 btree。
            # 位置对齐 PG 语法（也是 Tortoise BasePostgresSchemaGenerator 的做法）：
            #   CREATE [UNIQUE] INDEX 名 ON 表 [USING 方法] (列…) [WHERE …]
            using = f"USING {spec.index_type} " if spec.index_type else ""
            # 索引名加引号：自定义 name 可能含大写，不加引号会被 PG 折叠成小写，
            # 于是 pg_indexes 回读的名字与 desired 的 key 永远对不上——每次同步都
            # 重复 CREATE 并报 already exists。与 DROP 两处保持一致的引用方式。
            sql   = f'CREATE {kind} "{iname}" ON {cur} {using}({col_list}){spec.extra}'
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
