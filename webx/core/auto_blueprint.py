"""蓝图自动注册器。

核心功能：
  1. 扫描 controller 包，自动发现并注册 Sanic 蓝图。
  2. 按目录层级应用中间件，支持继承与模块级追加。
  3. 按路由 handler 的归属剔除「import 进来的别组蓝图」，杜绝跨组鉴权污染。

约定：
  - ``__mid__.py``：目录级中间件配置，导出列表变量 ``MIDDLEWARES``；
    子目录自动继承父目录的中间件链，可再追加。
  - ``__mid__``：模块级变量（写在具体 ``.py`` 里），仅对该模块的蓝图追加。
  - ``__blueprints__``：（可选）模块显式导出的蓝图列表；未定义则自动扫描模块属性。

与原实现的唯一差别、也是通用化的关键：**controller 包路径由调用方传入**，
不再写死 ``xymol.controllers``（原项目写死的那个）、也不再用 ``Path(__file__).parent.parent`` 推目录。
webx 是独立安装的包，那种写法只会扫到框架自己的安装目录。目录由
``importlib.util.find_spec`` 从包名反查，包与目录永远一致。
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
from pathlib import Path
from typing import Any, Iterable, Optional

from sanic import Blueprint, Sanic

logger = logging.getLogger(__name__)


class MiddlewareConfigError(Exception):
    """``__mid__.py`` 存在但加载失败（导入错误 / 语法错误 / 求值抛异常）。

    与「文件不存在」严格区分：不存在 = 该目录不追加中间件（合法配置，返回空列表）；
    加载失败 = **配置意图未知**，绝不可当成空列表放行——那会让整组路由裸奔。
    """


class DuplicateBlueprintError(Exception):
    """同一个 Blueprint 对象被两个控制器模块注册 —— 兜底守卫，正常路径不会触发。

    误 import 别组蓝图这条主路径，已由 `_blueprint_owner_matches` 的归属判定在
    上游消解（外来蓝图直接不收），故本异常**只剩两种触发情形**：

      1. 模块显式声明了 ``__blueprints__``，且里面写进了别人的蓝图。
         显式声明 = 完全信任作者、跳过归属判定，所以只能靠这里拦。
         那是明确的错误意图而非手滑，中止启动是对的。
      2. 无路由的蓝图（归属判定返回 None，按「拥有」保守处理）被多个模块收走。

    之所以中止启动而不是跳过：污染在检测时已经发生——``bp.middleware()`` 改的是
    那个共享对象本身，挂载先于注册，跳过也收不回。修法二选一：
      · 在该模块显式声明 ``__blueprints__ = [本模块自己的 bp]``；
      · 或改为 import 函数/常量，不要 import 蓝图对象。

    ⚠️ webx 特有的覆盖范围限制：``seen`` 是**单次 ``auto_register_blueprints`` 调用**
    内的状态，而 ``create_app`` 对每个内置路由组各调一次。故「显式 ``__blueprints__``
    写进了**另一次调用**所扫包的蓝图」这一种情形本守卫抓不到，Sanic 会静默跳过第二次
    ``app.blueprint()``。这条路径需要作者自觉，不在守卫范围内。
    """


class BlueprintNameConflictError(Exception):
    """两个**不同的** Blueprint 对象取了同一个名字。

    与 `DuplicateBlueprintError` 的区别：那个抓的是「同一个对象被两处发现」（跨组
    鉴权污染）；这个抓的是「两个各自独立的蓝图重名」——多包部署时最现实的撞车。

    蓝图名是**全局唯一命名空间**，与 URL 前缀完全无关：``Blueprint("user", "/api/user")``
    与 ``Blueprint("user", "/biz/user")`` 路径毫不相干，照样撞。Sanic 自己会抛
    ``AssertionError: Blueprint names must be unique``，但它只报名字、不报**哪两个
    模块**——单包时 grep 一下就找到，多包部署时得在几个包里翻。

    本异常在 ``app.blueprint()`` 之前拦截，用已有的归属判定反查出双方的定义处，
    把「名字撞了」变成「``a.b.user`` 与 ``x.y.user`` 撞了」。不限制任何命名自由，
    只是把已有信息暴露出来。
    """


class OrphanBlueprintError(Exception):
    """某个蓝图被所有模块判定为「不是我的」，最终无人注册 —— 接口会静默消失。

    这是归属判定（`_blueprint_owner_matches`）唯一可能引入的新故障：判据看的是
    路由 handler 的 ``__module__``，若某个蓝图的 handler 全部来自别的模块
    （例如 handler 由共享工厂函数生成、或蓝图在一个模块建、路由在另一个模块加），
    每个扫到它的模块都会认为「这不是我定义的」而跳过它。

    后果与漏挂鉴权同级危险，只是方向相反：**接口 404，且没有任何错误**。
    故扫描结束后统一核对：凡被跳过却从未被任何模块注册的蓝图，一律中止启动。
    修法：在真正拥有它的那个模块里显式声明 ``__blueprints__ = [该蓝图]``。

    豁免情形见 `auto_register_blueprints` 里的 ``_orphan_exemption``。
    """


def _blueprint_owner_matches(bp: Blueprint, module_name: str) -> Optional[bool]:
    """判断蓝图是否「定义在」``module_name``，判据是其路由 handler 的 ``__module__``。

    Blueprint 实例本身的 ``__module__`` 恒为 ``sanic.blueprints``（那是类的定义处），
    用不了；而路由 handler 是在定义蓝图的那个文件里写的，天然带着归属信息。

    【为什么这层过滤是必需的，而不是「报错就够了」】
    自动扫描分不清「我定义的」与「我 import 的」，一句 ``from ..task import *`` 就会
    把别组蓝图收进来，进而把本组中间件挂到它身上；``app.blueprint()`` 的二次注册被
    Sanic 静默跳过，于是**先注册的那条链胜出**。而扫描顺序是「本目录 .py 按文件名
    排序 → 再递归子目录」，父目录文件恒先于子目录，两个方向后果截然不同
    （sanic 25.12 实测）：

      · 严格组 import 宽松组（``api/sys/x.py`` ← ``api/task.py`` 的蓝图）：
        受害蓝图早已按宽松链注册完毕 → 运行时不受影响，只是静态链变脏。
      · 宽松组 import 严格组（``api/aaa.py`` ← ``api/sys/notice.py`` 的蓝图）：
        sys 蓝图在扫 ``api/`` 时被**提前注册**，那一刻链里还没有 inner 守卫 →
        **/api/sys/\\* 对所有登录用户敞开**。

    两者的区别仅仅是**文件名的字母序**——同一个笔误今天无害、明天改个文件名
    就变成鉴权失效。既然归属可判定，正确做法就是让这类事故**不会发生**，
    而不是发生后报错。

    Returns:
        True  —— 本模块贡献了至少一个 handler，视为拥有；
        False —— 蓝图有路由，但没有一个 handler 出自本模块 → 是 import 进来的；
        None  —— 蓝图尚无路由，无从判定（交给调用方按保守策略处理）。

    ⚠️ 用「本模块是否在贡献者之中」而非「贡献者是否唯一」：宽容的一侧是
    「多收一个」（后面还有 seen 查重兜底），严格的一侧是「少收一个」
    （接口静默消失，只能靠 OrphanBlueprintError 事后补救）。两害相权取前者。
    """
    routes = getattr(bp, "_future_routes", None)
    if routes is None:
        # 框架漂移：Sanic 改了这个私有属性名。判定退化 → 一切蓝图按「拥有」处理
        # → 静默退回修复前的污染行为，而这是条鉴权相关的路径。fail-open 可以接受，
        # **静默** fail-open 不行，故必须留声。
        logger.warning(
            "Blueprint 缺少 _future_routes 属性，蓝图归属判定已失效"
            "（Sanic 版本变更？）——跨模块 import 蓝图将不再被自动剔除，"
            "仅剩 seen 查重兜底"
        )
        return None

    mods = {getattr(r.handler, "__module__", None) for r in routes}
    mods.discard(None)
    if not mods:
        return None  # 蓝图尚无路由：无从判定，非异常
    return module_name in mods


def _owner_modules(bp: Blueprint) -> set[str]:
    """蓝图的 handler 分别来自哪些模块（= 它真正的归属方）。孤儿诊断用。"""
    return {
        m
        for m in (
            getattr(r.handler, "__module__", None)
            for r in getattr(bp, "_future_routes", ()) or ()
        )
        if m
    }


def _under(module: str, root: str) -> bool:
    """``module`` 是否等于 ``root`` 或落在它之下（按包路径，而非字符串前缀）。"""
    return module == root or module.startswith(root + ".")


def _deduplicate(mids: list[Any]) -> list[Any]:
    """保持原始顺序的去重。

    ⚠️ 不用 ``dict.fromkeys``：它要求元素可哈希，而 ``MIDDLEWARES`` 里混入 dict/list
    （配置写错）会抛 ``TypeError: unhashable type``。该调用位于 ``_scan_directory``
    的 try 之外，异常会**直接中止整个应用启动**——既违反「配置有问题就跳过那一组」
    的契约，也绕过了组级失败隔离。中间件恒为函数对象，按 identity 去重与原语义
    等价（函数的 ``__eq__`` 即 ``is``），且对任何元素都不会抛。
    非可调用元素继续交给 ``_apply_and_register`` 逐个 warning 跳过。
    """
    out: list[Any] = []
    for m in mids:
        if not any(m is x for x in out):
            out.append(m)
    return out


def _package_dir(package: str) -> Path:
    """由包名反查其文件系统目录。

    比 ``__file__`` 上溯可靠：无论包被 ``pip install -e`` 还是普通安装、
    无论进程 CWD 在哪，find_spec 给出的都是该包真实所在目录。
    """
    spec = importlib.util.find_spec(package)
    if spec is None or not spec.submodule_search_locations:
        raise ImportError(f"controller 包不存在或不是包：{package}")
    return Path(list(spec.submodule_search_locations)[0])


def _load_mid_file(dir_path: Path, module_prefix: str) -> list[Any]:
    """加载目录下 ``__mid__.py`` 的 MIDDLEWARES；文件不存在返回空列表。

    Raises:
        MiddlewareConfigError: 文件存在但加载失败。
            ⚠️ 此处**不可**吞掉任何异常返回 ``[]``：文件不存在的情况上面已经拦掉了，
            剩下能走到 except 的全是真实故障（函数名写错、依赖缺失、语法错误）。
            ``except ImportError: return []`` 那种写法会让「``__mid__.py`` 里一个笔误」
            静默变成「该组全部路由无鉴权且应用照常启动」——鉴权配置的失败方式
            只能是「不放行」，不能是「当作没配」。
    """
    if not (dir_path / "__mid__.py").exists():
        return []

    module_name = f"{module_prefix}.__mid__"
    try:
        module = importlib.import_module(module_name)
        mids = list(getattr(module, "MIDDLEWARES", []))
        logger.debug("加载目录中间件: %s -> %d 个", module_name, len(mids))
        return mids
    except Exception as e:
        logger.error("加载中间件配置 %s 失败: %s", module_name, e)
        raise MiddlewareConfigError(f"{module_name}: {e}") from e


def auto_register_blueprints(
    app: Sanic,
    controller_package: str,
    controller_dir: Optional[Path] = None,
    global_middleware: Iterable[Any] = (),
) -> int:
    """扫描 controller 包，注册蓝图并应用层级中间件。

    Args:
        app: Sanic 应用实例。
        controller_package: 控制器根包名，如 ``"app.controller"``。
        controller_dir: 目录路径；默认由包名反查（正常情况无需传）。
        global_middleware: **全站级**中间件，作为本次扫描继承链的最外层起点。

            中间件共有四层，本参数补上此前缺失的最外一层::

                全站级(本参数) → 包级(根 __mid__.py) → 目录级 → 模块级(__mid__ 变量)

            它跟随**部署**而非模块：``create_app`` 对每个内置组、每个业务包各调用
            本函数一次，同一份 global 传给每一次调用，于是内置路由与业务路由**都**
            吃得到——这正是此前的缺口（业务根 ``__mid__.py`` 盖不到 ``/api/user``）。

            ⚠️ 只放**幂等的上下文注入**（request_id / client_ip / lang / trace）。
            **做准入决策的一律不准进**（鉴权、限流、权限）：全站链对所有包生效，
            包括 ``/auth/login`` 与 ``/public/files`` 这些公开端点，把 auth 放进来
            一行配置就锁死全站。

            迁移安全：与包级 ``__mid__.py`` 重复声明同一个函数**不会双跑**，
            ``_deduplicate`` 按 identity 去重（见其 docstring）。

            边界：蓝图级中间件在路由匹配成功后才执行，故**不覆盖 404**。需要覆盖
            404 的（访问日志、trace）属于 server 层关注点，用 Sanic 原生的 app 级
            中间件，不要混进 ``__mid__`` 体系。

    Returns:
        成功注册的蓝图数量。

    Raises:
        OrphanBlueprintError: 蓝图被判定为外来而跳过，却无人注册（接口静默消失）。
        DuplicateBlueprintError: 同一 Blueprint 对象被两个模块发现（跨组鉴权污染）。
        BlueprintNameConflictError: 两个不同的蓝图取了同名（多包部署最常见的撞车）。
        MiddlewareConfigError: **根** ``__mid__.py``（即 ``controller_package`` 自己那份）
            加载失败。根配置决定所有组的基础中间件链，它失败意味着每一组都要被跳过 →
            起来一个零路由的空服务，还会骗过健康检查（进程活着、端口通）。这种状态
            没有任何「局部可用」价值，故不降级、直接崩。子目录失败则**不**走这里
            （见 ``_scan_directory``：只跳那一棵子树，父组与兄弟组照常注册）。

            ⚠️ 「根」是相对本次调用而言的：``create_app`` 对每个内置路由组各调一次，
            所以 ``webx/controller/api/__mid__.py`` 在 ``builtin_controller=True`` 时是
            子目录（失败→跳过该组），在 ``builtin_controller=["api"]`` 时是根（失败→抛）。
    """
    controller_dir = controller_dir or _package_dir(controller_package)

    if not controller_dir.exists():
        logger.warning("控制器目录不存在: %s", controller_dir)
        return 0

    logger.info("🚀 开始扫描控制器: %s (%s)", controller_package, controller_dir)

    # 1. 全站链 + 根目录 __mid__.py（后者失败不降级，理由见 docstring）
    #    这一句刻意留在 _scan_directory 之外：它让**根**的加载失败成为 fail-fast，
    #    而子目录失败只跳该子树。全站链拼在最前 → 继承顺序自动是「从外到内」。
    base_mids = list(global_middleware) + _load_mid_file(controller_dir, controller_package)

    # 2. 递归扫描（组级失败在 _scan_directory 内就地记 ERROR 并跳过整棵子树）
    #    三份状态整棵树共享：
    #      seen          —— id(bp) -> (首次注册它的模块, bp)，拦跨模块 import 的鉴权污染
    #      skipped       —— id(bp) -> (跳过它的模块, bp)，供扫描后核对孤儿
    #      failed_groups —— __mid__.py 加载失败而整组跳过的模块前缀，孤儿核对要据此让步
    seen: dict[int, tuple[str, Blueprint]] = {}
    skipped: dict[int, tuple[str, Blueprint]] = {}
    failed_groups: set[str] = set()
    count = _scan_directory(
        app,
        controller_dir,
        controller_package,
        base_mids,
        seen,
        skipped,
        failed_groups,
    )

    # 3. 孤儿核对：被判定为「外来」而跳过的蓝图，必须在别处真的被注册过。
    #    否则说明归属判定把它误伤了 —— 那是**接口静默消失**，比多注册一次更难发现。
    orphans = [(who, bp) for bid, (who, bp) in skipped.items() if bid not in seen]

    def _orphan_exemption(bp: Blueprint) -> Optional[str]:
        """返回豁免理由；``None`` 表示这是真误伤，必须中止启动。

        三条豁免，都是「无人注册」属于**既定且正确**的结果，抛异常反而严格劣化：

          1. 归属不可判（无 handler 带 ``__module__``）—— 判据本身失效，
             此时中止启动只是把「判不了」升级成「起不来」，无助于诊断。
          2. owner 目录因 ``__mid__.py`` 加载失败而整组被跳过：那一组本就该 404
             不可达（tests/test_auto_blueprint_mid_failure 契约 3）。若还抛异常，
             就把「一个组缺失、服务照常」升级成「整个应用起不来」。
          3. **webx 特有** —— owner 不在本次扫描根之下。``create_app`` 对每个内置
             路由组各调一次本函数，``seen`` 是调用内局部的；``api`` 组的文件 import
             了 ``auth`` 组的蓝图时，本次调用只看得见「被跳过」，看不见「另一次调用
             已注册」。本次调用对它没有裁判权，只能留声。
             注意：归属**过滤**本身是跨调用成立的（外来蓝图压根不会进
             ``_apply_and_register``），所以鉴权污染仍被杜绝，让步的只是核对。
        """
        owners = _owner_modules(bp)
        if not owners:
            return "无法判定归属（该蓝图没有带 __module__ 的路由）"
        if any(_under(m, g) for m in owners for g in failed_groups):
            return "owner 目录的 __mid__.py 加载失败，整组已按契约跳过——先修那个 __mid__.py"
        if not any(_under(m, controller_package) for m in owners):
            return f"owner 不在本次扫描根 {controller_package} 之下，由另一次注册调用负责"
        return None

    real: list[tuple[str, Blueprint]] = []
    for who, bp in orphans:
        reason = _orphan_exemption(bp)
        if reason is None:
            real.append((who, bp))
        else:
            logger.error(
                "蓝图 %r (%s) 未被本次扫描注册：%s 只是 import 了它。%s",
                bp.name,
                bp.url_prefix or "/",
                who,
                reason,
            )

    if real:
        detail = "; ".join(
            f"{bp.name!r} ({bp.url_prefix or '/'}) 被 {who} 跳过" for who, bp in real
        )
        raise OrphanBlueprintError(
            f"下列蓝图被判定为外来而跳过，却没有任何模块注册它，接口将不可达：{detail}"
            f"。修法：在真正拥有它的模块里显式声明 __blueprints__ = [该蓝图]。"
        )

    logger.info("✅ 蓝图自动注册完成，共注册 %d 个蓝图", count)
    return count


def _scan_directory(
    app: Sanic,
    dir_path: Path,
    module_prefix: str,
    inherited_mids: list[Any],
    seen: Optional[dict] = None,
    skipped: Optional[dict] = None,
    failed_groups: Optional[set] = None,
) -> int:
    """递归扫描目录，加载蓝图并应用中间件链。

    失败隔离的两个方向由本函数顶部这个 try/except 一并给出，依赖其执行顺序
    （① 载本目录 ``__mid__`` ② 注册本目录 ``.py`` ③ 递归子目录）：

      - **子目录失败** → 异常发生在子目录自己那次调用的 ① 里，就地 return 0；
        父目录的 ② 早已完成、③ 的循环继续走下一个兄弟 → 父组与兄弟组毫发无损。
      - **本目录失败** → ②③ 都不执行 → 子目录压根没机会注册。这条是关键：
        子目录继承父目录的中间件，若父组失败却仍递归进去，子组会带着**缺失的
        继承链**上线——看似正常、实则少一道鉴权，比整组消失危险得多。

    选「整组跳过」而非「整个服务崩」：路由压根不注册 → 404 不可达，同样杜绝
    鉴权旁路，但爆炸半径只限于该组。

    Args:
        seen / skipped / failed_groups: 整棵树共享的三份状态，语义见
            `auto_register_blueprints`。默认 ``None`` 时就地新建——仅为兼容直接
            调用本函数的测试；正常入口一律由 `auto_register_blueprints` 传入，
            否则跨目录的重复与孤儿都无法被发现。
    """
    if seen is None:
        seen = {}
    if skipped is None:
        skipped = {}
    if failed_groups is None:
        failed_groups = set()

    try:
        dir_mids = _load_mid_file(dir_path, module_prefix)
    except MiddlewareConfigError as e:
        logger.error(
            "⛔ 中间件配置加载失败，跳过整组: %s —— 该目录及其所有子目录一律不注册路由"
            "（对外表现为 404 不可达）。原因: %s",
            module_prefix,
            e,
        )
        # 登记失败组：孤儿核对要据此区分「归属判定误伤」与「owner 组本就被跳过」，
        # 后者是既定且正确的结果，不该升级成整个应用启动失败。
        failed_groups.add(module_prefix)
        return 0

    # 中间件链 = 父级继承 + 本目录（去重保序）
    effective_mids = _deduplicate(inherited_mids + dir_mids)

    count = 0

    # 排序保证加载顺序确定性（跨机器/文件系统一致）
    py_files = sorted(f for f in dir_path.iterdir() if f.is_file() and f.suffix == ".py")
    for py_file in py_files:
        # 跳过私有文件（含 __init__.py / __mid__.py）
        if py_file.name.startswith("_"):
            continue

        file_module_name = f"{module_prefix}.{py_file.stem}"
        try:
            module = importlib.import_module(file_module_name)

            # 发现蓝图（外来的就地剔除，登记待核对）
            blueprints, foreign = _find_blueprints(module)
            for fbp in foreign:
                # 记 file_module_name 而非 module_prefix：孤儿报错要能点到**哪个文件**
                # 做的 import，只报出目录等于让人自己去翻。
                skipped.setdefault(id(fbp), (file_module_name, fbp))

            if not blueprints:
                continue

            # 模块级追加：只影响本模块的蓝图
            module_mids = list(getattr(module, "__mid__", []))
            final_mids = _deduplicate(effective_mids + module_mids)

            for bp in blueprints:
                _apply_and_register(app, bp, final_mids, file_module_name, seen)
                count += 1
        except Exception as e:
            logger.error("❌ 加载控制器模块 %s 失败: %s", file_module_name, e)
            # 启动期直接失败，好过带病运行
            raise

    for sub_dir in sorted(d for d in dir_path.iterdir() if d.is_dir()):
        if sub_dir.name.startswith("_") or sub_dir.name == "__pycache__":
            continue
        count += _scan_directory(
            app,
            sub_dir,
            f"{module_prefix}.{sub_dir.name}",
            effective_mids,
            seen,
            skipped,
            failed_groups,
        )

    return count


def _find_blueprints(module) -> tuple[list[Blueprint], list[Blueprint]]:
    """从模块中找出蓝图。

    ``__blueprints__`` 支持三种形态（显式声明 = 完全信任作者，**不做**归属判定）：

      - 蓝图对象或其列表/元组 —— 最常见的写法
      - **可调用对象**（工厂）—— 每次注册时调用一次，返回蓝图或蓝图列表

    工厂形态是「同一 controller 包注册进多个 app」的唯一正确写法：模块只 import
    一次，模块级的蓝图对象随之固化，第二个 app 复用它会撞 RouteExists。写成
    ``__blueprints__ = lambda: [make_xxx_blueprint()]`` 则每个 app 拿到全新实例，
    也因此天然不会被 seen 查重误判为重复。

    未声明 ``__blueprints__`` 时自动扫描模块属性，**并剔除 import 进来的外来蓝图**。
    ⚠️ 这层归属过滤是必需的，不是优化：自动扫描看的是模块的全部属性，分不清
    「我定义的」与「我 import 的」。一句 ``from ..task import *`` 就会把别组的蓝图
    收进来，进而把本组的中间件挂到它身上——后果见 `DuplicateBlueprintError`。
    以 handler 的 ``__module__`` 判定归属后，正常的跨模块复用（import 函数、常量，
    甚至误 import 了蓝图）都能被无声跳过，这类事故从「需要检测」变成「不会发生」。

    Returns:
        ``(owned, foreign)``：本模块拥有的蓝图、被判定为外来而跳过的蓝图。
        foreign 交由调用方登记，供扫描结束后的孤儿核对（见 `OrphanBlueprintError`）。
        显式声明路径的 foreign 恒为空。
    """
    # 1. 显式声明：作者已经指名道姓，不再猜归属
    explicit = getattr(module, "__blueprints__", None)
    if explicit is not None:
        if callable(explicit) and not isinstance(explicit, Blueprint):
            explicit = explicit()
        owned = list(explicit) if isinstance(explicit, (list, tuple)) else [explicit]
        return owned, []

    # 2. 自动扫描 + 归属过滤
    owned = []
    foreign = []
    for attr_name in dir(module):
        if attr_name.startswith("_"):
            continue
        attr = getattr(module, attr_name)
        if not isinstance(attr, Blueprint):
            continue
        # None（无路由，无从判定）按拥有处理：保持历史行为，由 seen 查重兜底
        if _blueprint_owner_matches(attr, module.__name__) is False:
            foreign.append(attr)
            logger.debug(
                "跳过外来蓝图 %r：它由别的模块定义，只是被 %s import 进来"
                "（handler 不出自本模块）",
                attr.name,
                module.__name__,
            )
            continue
        owned.append(attr)

    return owned, foreign


def _apply_and_register(
    app: Sanic,
    bp: Blueprint,
    mids: list[Any],
    source: str,
    seen: Optional[dict] = None,
) -> None:
    """把中间件挂到蓝图上并注册进 app。

    ⚠️ 挂中间件之前**先查重**：``bp.middleware()`` 一旦调用就改写了那个对象，
    对跨模块共享的蓝图而言就是改了别的组的鉴权链，事后无法回滚。
    """
    if seen is not None:
        # 值里带上 bp 本身：id() 只在对象存活期间唯一，顺手持有一个强引用，
        # 杜绝「前一个蓝图被回收、新对象复用同一 id」的理论误判。
        owner = (seen.get(id(bp)) or (None,))[0]
        if owner is not None:
            raise DuplicateBlueprintError(
                f"蓝图 {bp.name!r} ({bp.url_prefix or '/'}) 同时被 {owner} 与 {source} 发现"
                f"——多半是 {source} 里 import 了 {owner} 的蓝图对象，"
                f"会把 {source} 所在组的中间件挂到 {owner} 的蓝图上（跨组鉴权污染）。"
                f"修法：在 {source} 中显式声明 __blueprints__ = [本模块自己的蓝图]。"
            )
        seen[id(bp)] = (source, bp)

    # 名字冲突：查的是 app.blueprints 而非 seen，故**跨扫描根也能抓到**
    # （内置组与业务包各走一次调用，seen 不共享，但 app 是同一个）。
    existing = app.blueprints.get(bp.name)
    if existing is not None and existing is not bp:
        owners = sorted(_owner_modules(existing)) or ["未知模块"]
        raise BlueprintNameConflictError(
            f"蓝图名 {bp.name!r} 冲突：{', '.join(owners)} 与 {source} 各定义了一个同名蓝图"
            f"（前者 {existing.url_prefix or '/'}，后者 {bp.url_prefix or '/'}）。"
            f"蓝图名是全局唯一命名空间，与 URL 前缀无关——路径不同也照样撞。"
            f"修法：给其中一个改名，多包部署建议按包加前缀。"
        )

    for mid in mids:
        if callable(mid):
            bp.middleware(mid)
        else:
            logger.warning("跳过无效的中间件对象: %r (来源: %s)", mid, source)

    app.blueprint(bp)
    logger.debug("📦 注册蓝图: %s (%s) [来自 %s]", bp.name, bp.url_prefix or "/", source)
