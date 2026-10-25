"""
日志截断写入器
实现"首 N 行 + 尾 M 行"的日志截断策略，防止磁盘打满
"""

import codecs
import re
from collections import deque
from functools import lru_cache
from itertools import groupby
from pathlib import Path
from typing import Callable, Optional, Sequence


# ==================== 前端可见日志脱敏 ====================
# worker 子进程 stdout 会混入与云端交互的「基础设施日志」（结果上传 / 字段回调 / 产物下载等）。
# 这些行对最终用户无意义，且会暴露内部接口路径 / 完整 URL / 上传成败与结果——一旦随
# get_truncated_content 上报并在前端「查看日志」展示即构成信息泄露（尤其 API 路径）。
# 策略：命中基础设施日志的整行剔除（覆盖「上传到哪个接口 / 是否成功 / 上传结果」全部信息）；
# 对残留的 URL / 内部 API 路径再兜底遮蔽，绝不外泄具体地址。
# 仅作用于「面向前端展示」的截断内容出口（get_truncated_content）；worker 本机 systemd 日志
# 与本地全量日志文件仍保留原文，便于排障。

# 命中即整行剔除（真正的上传/回调/下载日志必带其一；裸 URL 不在此列，改走兜底遮蔽，
# 以免误删正文里无害的含 URL 行）
_REDACT_DROP_RE = re.compile(
    r"/(?:internal|public)/tasks/"               # 内部/公开任务 API 路径
    r"|results/upload"                            # 结果上传端点
    r"|download_url"                              # 上传回传的下载地址字段
    r"|结果文件上传|任务字段更新|产物下载|产物 404"  # sdk.result_client 的内部回调日志前缀
)

# ==================== 业务脱敏：redactors 注册表 ====================
# 「哪些行对最终用户是噪音/不该外泄」取决于具体任务类型（某个外部程序的启动横幅、
# 内部标记行……），不属于通用机制。这类规则以 `redactors: list[Callable[[str], str]]`
# 的形式注册（来自 TaskKindPlugin.redactors，或直接传给 ConcurrentLogWriter）：
# 每个函数接收**整段日志文本**、返回处理后的文本，在内置规则**之前**依次执行。
#
# 为什么放在内置规则之前：业务规则可能依赖原文里的 URL 等锚点（例如某段横幅以
# 一行含 URL 的文字收尾），若先跑下面的 URL 遮蔽，锚点会变成 `[redacted]` 而落空。
#
# 约束：redactor 必须是纯函数且不得抛异常以外的副作用；抛出的异常会被吞掉并跳过该
# redactor（脱敏失败不得影响日志推送，但内置规则仍会执行）。

Redactor = Callable[[str], str]

# 兜底遮蔽（未被整行剔除时，仍不外泄具体地址）
_REDACT_URL_RE = re.compile(r"https?://[^\s'\"]+")
_REDACT_PATH_RE = re.compile(r"/(?:internal|public)/tasks/[^\s'\"]*")


# ==================== 文件系统绝对路径脱敏 ====================
# 日志（尤其 traceback / conda run 命令回显）会带出**本机布局**：worker 安装路径、
# conda 环境前缀、家目录、任务目录——含用户名、盘符、项目目录名，属系统隐私，不该进前端。
#
# 两层，顺序不可颠倒：
#   ① 已知根【精确前缀替换】——这些路径 worker 自己就知道（构造期由 __main__.build_executor
#      经 build_redact_roots() 传入），无需猜测，替换成语义标签后**相对结构完整保留**，
#      诊断价值不损失：`[WORKER]/executor.py`、`[ENV]/lib/python3.11/.../site.py`。
#   ② 未命中的绝对路径【折叠成末尾两段】——第三方程序打印的安装路径
#      worker 无从得知（且 conda_root 探测有回退、可能失手），故留一道兜底，保证
#      「即使已知根失手也不外泄本机布局」。折叠而非全抹：`…/pkg/module.py` 仍能定位问题。
_FOLD_KEEP_SEGMENTS = 2  # 兜底折叠时保留的末尾路径段数

# 【单一来源】"一个路径段允许哪些字符"——下面三处**必须**共用它，否则差集里的字符会被
# 当成段边界，产生**段中途替换**：既改花正文、又只遮住半个名字。历史上已踩过三次：
#   • `/home/ps` 命中 `/home/psdz2/x` → `[HOME]dz2/x`
#   • 边界用 ASCII 集时，`/home/user` 命中 `/home/user张三/x` → `[HOME]张三/x`
#   • 家目录段不含 `:` 时，`/home/user:x/y` → `[HOME]:x/y`
# `\w` 在 Python3 下 **Unicode 感知**（中文/日文目录名不能漏，那恰恰最可能含真名）；
# `:` `+` `@` 均为合法路径字符。**空格刻意不含**，理由见下方 `_FS_PATH_RE` 附近说明。
_SEG_CHARS = r"[\w.+\-@:]"

# 绝对路径匹配。**只对名单内的顶级目录生效**，其余一律不动：
#   /root /media /home /var /etc /opt /usr
# ⚠️ 名单里的是**要被遮掉**的根，不是放行的根——命中即折叠，不在名单里的原样保留。
# 为什么限定顶级目录而不是"任何绝对路径"：兜底层要防的是**本机布局与身份泄露**，而这类
# 路径集中在上面几个根下（`/home/<user>`、`/media/<user>/<disk>/<user>/项目`、`/root`，
# 以及 `/opt`、`/usr` 下的部署与依赖目录）。放开成"任何绝对路径"则会把 `/srv/...`、
# `/tmp/...`、`/mnt/...` 也卷进来，用更大的误伤面去换很低的收益。
# 增删根目录只改 _FS_PATH_ROOTS 一处，正则由它拼出。
#
# 其余三个约束仍缺一不可：
#   • 前一字符不能是 \w / ~ / ]  —— 挡住 `kJ/mol/nm`（前有 J）、`2026/07/29`（前有数字）、
#     以及已被①替换成 `[WORKER]/tasks/...` 的结果（前有 ]），否则会二次折叠标签后的安全路径。
#   • 根之后至少还有 2 段（合计 ≥3 段）—— `/var/log` 这类短路径不折。
#   • URL 已在前一步被整体遮蔽，不会走到这里。
# 根名后面必须紧跟 `/`（由后面的 `(?:/...)` 保证），故 `/homework/a/b/c` 不会误命中。
_FS_PATH_ROOTS = ("root", "media", "home", "var", "etc", "opt", "usr")
_FS_PATH_RE = re.compile(
    r"(?<![\w~\]])/(?:" + "|".join(_FS_PATH_ROOTS) + r")(?:/" + _SEG_CHARS + r"+){2,}/?"
)

# 【已评估后不做·勿再加】含**空格**的裸路径（`/opt/My App/bin/tool`）不折叠。
# 两条路都不划算：① 把空格放进段内字符集 → `see /tmp/x and /var/y` 被当成一条 4 段
# 路径、把 `and` 折进去，改花正文；② 对「双引号内以 / 开头的整串」单独处理 → 误伤面
# 扩到所有引号包裹的路径形字符串（如日志里打印的路由模式 `"/api/tasks/<id>/files"`
# 会被折成 `"…/<id>/files"`，丢掉有用信息）。
# 而专门部署的服务里带空格的安装路径本就极罕见——用一片新的误伤面换近乎为零的收益，
# 不划算。宁可漏遮这一种，也不动正文。

# 通用家目录形态。①只认得**本机当前用户**的家目录，但日志里可能出现别的用户/别的机器的
# 家目录（第三方软件安装在 /home/someone/tool 之类），而②折叠保留末尾两段时，用户名恰好
# 落在倒数第二段 → `/home/wangyong/data` 会折成 `…/wangyong/data`，用户名仍然漏出去。
# 故在 ①② 之间补这一道：任何 /home/<name> 或 /Users/<name> 一律收敛为 [HOME]。
# 用户名段同样用 _SEG_CHARS（单一来源）：窄于它就会在 `/home/user:x/y` 上中途命中，
# 产出 `[HOME]:x/y`——与前缀替换那处是同一类缺陷。
_GENERIC_HOME_RE = re.compile(r"(?<![\w~\]])/(?:home|Users)/" + _SEG_CHARS + r"+")

# 路径边界断言：前缀之后不能再接路径名字符，否则 `/home/ps` 会把 `/home/psdz2/x`
# 改成 `[HOME]dz2/x`——既破坏正文又只遮了一半。必须按「路径段」而非「子串」替换。
# 字符集由 _SEG_CHARS 派生（单一来源）：与段内字符集不一致时，差集里的字符会被当成
# 段边界而放行替换，重新打开同一个洞。
# 已知取舍：冒号一律算路径字符，故 `<已知根>: <消息>` 不会被替换成语义标签
# （`/media/.../worker: denied` → 兜底折叠成 `…/proj/worker: denied`）。**不泄露**，
# 只是标签退化——真实的根都够深且以名单内顶级目录开头，兜底必然接住。
# 要恢复标签需按「冒号后是否还跟路径字符」二分，代价是把常量拆成两级 + 嵌套前瞻，
# 为一个诊断细节不值，故不做。
_PATH_BOUNDARY = r"(?!" + _SEG_CHARS + r")"


@lru_cache(maxsize=128)
def _compile_roots(
    roots: tuple[tuple[str, str], ...]
) -> tuple[tuple[str, "re.Pattern[str]", str], ...]:
    """把 (前缀, 标签) 编译成 (前缀, 带路径边界的正则, 标签)，按 roots 缓存。

    roots 是不可变元组、可哈希；每任务的 roots 只差一个 [TASK_DIR] 头，缓存条目数
    ≈ 并发任务数，128 远够用。

    ⚠️ 返回**三元组**：``prefix`` 原样带着，供逐行时先做 C 层 ``in`` 判断——正则
    ``sub`` 即便一个都不匹配也要把整行扫完，而不含任何已知根的行占绝大多数。
    """
    return tuple(
        (prefix, re.compile(re.escape(prefix) + _PATH_BOUNDARY), label)
        for prefix, label in roots
    )


def _fold_path(m: "re.Match[str]") -> str:
    """把绝对路径折叠成 ``…/末尾两段``（保留尾部斜杠形态）。"""
    raw = m.group(0)
    trailing = "/" if raw.endswith("/") else ""
    segs = [s for s in raw.split("/") if s]
    return "…/" + "/".join(segs[-_FOLD_KEEP_SEGMENTS:]) + trailing


def build_redact_roots(
    *,
    work_dir: str = "",
    conda_root: str = "",
    channels: Optional[dict] = None,
    worker_root: Optional[Path] = None,
    home: Optional[Path] = None,
    script_root: Optional[Path | str] = None,
) -> tuple[tuple[str, str], ...]:
    """收集本机的「已知绝对路径根」，供 redact_for_frontend 做精确前缀替换。

    调用点唯一：``app.build_executor()``——那里 work_dir / conda_root / channels
    三个值都已定版（``--work-dir`` 覆盖与 load_channel_config 都在其前），故不存在
    「先构造后覆盖」的窗口；构造期一次性传入也保证了增量日志从第一次推送就已脱敏。

    Args:
        work_dir:    任务工作目录根（worker_settings.WORK_DIR）
        conda_root:  conda 安装根（channel_loader 的 CONDA_ROOT）
        channels:    CHANNELS 配置，用于按 env_name 构造各计算环境前缀
        worker_root: 本包所在目录，默认由本文件位置推导
        home:        家目录，默认 Path.home()
        script_root: 任务脚本根目录（channel 配置所在目录）

    Returns:
        ``((前缀, 标签), ...)``，**已按前缀长度倒序**——长前缀必须先匹配，
        否则 ``{conda_root}`` 会先吃掉 ``{conda_root}/envs/{env}``、``{work_dir}`` 会先吃掉任务目录。
    """
    roots: list[tuple[str, str]] = []

    def add(p, label: str) -> None:
        """登记一个根（同时登记其 resolve 形态：家目录/工作目录可能是符号链接，
        子进程打印的往往是解析后的路径）。过短/根目录一律丢弃，避免把 `/` 整个抹掉。"""
        if not p:
            return
        for cand in {str(p), str(Path(p).expanduser()), str(Path(p).expanduser().resolve())}:
            cand = cand.rstrip("/")
            if len(cand) > 1:
                roots.append((cand, label))

    # 计算环境前缀：conda 走 {root}/envs/{name}；uv 的 env_name 本身就是 python 路径
    for cfg in (channels or {}).values():
        if not isinstance(cfg, dict):
            continue
        env_name = str(cfg.get("env_name") or "")
        if not env_name:
            continue
        if cfg.get("env") == "conda":
            if conda_root:
                add(Path(conda_root) / "envs" / env_name, "[ENV]")
        elif cfg.get("env") == "uv":
            add(env_name, "[ENV]")

    add(conda_root, "[CONDA]")
    add(work_dir, "[WORK_DIR]")
    # worker 包目录（本文件在 webx_worker/log_writer.py → parent = webx_worker/）
    add(worker_root or Path(__file__).resolve().parent, "[WORKER]")
    # 任务脚本根目录（channel 配置所在目录）：脚本路径会出现在 traceback 里
    add(script_root, "[SCRIPTS]")
    add(home or Path.home(), "[HOME]")

    # 去重：同一前缀被多个根登记时**保留首次**（add 顺序即「具体 → 泛化」：
    # ENV → CONDA → WORK_DIR → WORKER → SCRIPTS → HOME，例如 WORK_DIR 恰好等于家目录时，
    # 标成 [WORK_DIR] 比 [HOME] 更有信息量）。
    uniq: dict[str, str] = {}
    for prefix, label in roots:
        uniq.setdefault(prefix, label)
    # 按长度倒序：长前缀优先，否则短前缀会先命中、把长前缀的语义吃掉
    return tuple(sorted(uniq.items(), key=lambda kv: len(kv[0]), reverse=True))


def redact_for_frontend(
    text: Optional[str],
    roots: tuple[tuple[str, str], ...] = (),
    redactors: Sequence[Redactor] = (),
) -> Optional[str]:
    """剔除/遮蔽面向前端日志中的内部接口路径、URL、上传回调结果与本机文件系统路径。

    - 业务 redactors：来自插件的整段文本变换（**须最先执行**，见上文说明）。
    - 整行剔除：命中 _REDACT_DROP_RE 的上传/回调/下载操作。
    - 兜底遮蔽：其余行残留的完整 URL、``/internal|public/tasks/`` 路径替换为 ``[redacted]``。
    - 路径脱敏：``roots`` 里的已知根精确替换为语义标签；剩余绝对路径折叠成 ``…/末尾两段``。
    空/None 原样返回。

    Args:
        text:      待脱敏文本
        roots:     ``build_redact_roots()`` 的产物（**须已按长度倒序**）；空元组 = 只做已有的
                   URL/接口脱敏 + 绝对路径折叠（未登记根时的降级形态，仍不外泄本机布局）
        redactors: 业务脱敏函数列表（见 ``Redactor``），在内置规则之前依次执行
    """
    if not text:
        return text
    # ⚠️ 业务 redactor 必须在**最前面**：它们可能依赖原文里的 URL 作锚点，
    # 若先跑下面的 URL 遮蔽，锚点会变成 `[redacted]` 而落空。
    for fn in redactors:
        try:
            text = fn(text)
        except Exception:  # noqa: BLE001 —— 脱敏失败不得影响日志推送
            continue
    # 编译好的根提到循环外：lru_cache 命中也仍是每行一次函数调用 + 元组构造，
    # 而 roots 在整段脱敏期间是常量。
    compiled_roots = _compile_roots(roots)
    kept: list[str] = []
    for line in text.splitlines():
        if _REDACT_DROP_RE.search(line):
            continue
        line = _REDACT_URL_RE.sub("[redacted]", line)
        line = _REDACT_PATH_RE.sub("[redacted]", line)
        # ① 已知根精确替换（长前缀优先，见 build_redact_roots 的排序约定）。
        #    先用廉价的 C 层子串判断挡掉绝大多数不含该根的行，命中才走带边界的正则——
        #    `sub` 即便零匹配也要把整行扫完，而不含已知根的行占绝大多数。
        for prefix, pattern, label in compiled_roots:
            if prefix in line:
                line = pattern.sub(label, line)
        # ①.5 通用家目录形态（别的用户/别的机器的家目录，①认不出）
        line = _GENERIC_HOME_RE.sub("[HOME]", line)
        # ② 兜底：未被任何已知根命中的绝对路径折叠
        line = _FS_PATH_RE.sub(_fold_path, line)
        kept.append(line)
    # 末尾再收敛一次空行：上面的整行剔除（基础设施日志）与业务 redactor 的块剔除，
    # 都会制造出连续空白段（原本被删行分隔的两个空行就此相邻）。collapse 幂等，重跑无害。
    return collapse_blank_runs("\n".join(kept))


# ==================== 连续空行收敛 ====================
_MAX_BLANK_RUN = 2


def collapse_blank_runs(text: str) -> str:
    """把 **连续 ≥2 个空行**合并为 1 个（单个空行原样保留）。

    整块剔除必然在原地留下大片空白。合并时**保留该段的第一行原文**，而不是写入一个
    新的 ``""``——空行可能带尾随空格，替换就成了「改写」。判空用 ``strip()``
    （纯空白即算空行），保留的却是原始那一行，两者不矛盾。
    """
    out: list[str] = []
    for is_blank, grp in groupby(text.split("\n"), key=lambda line: not line.strip()):
        chunk = list(grp)
        out.extend(chunk[:1] if is_blank and len(chunk) >= _MAX_BLANK_RUN else chunk)
    return "\n".join(out)


# ==================== 回车（\r）行装配 ====================
# 一些程序（如 `mdrun -v`）用 \r 把进度"原地刷新"在同一行上，长时间不输出 \n。
# 若把 \r 当换行处理，一次 md 会产出成千上万行几乎相同的进度行，既撑爆截断缓冲的
# 尾部窗口（把真正有用的收尾输出挤掉），又浪费上传流量。
# 正确做法是还原终端语义：\r = 光标回到行首、后续字符原地覆盖，于是整段进度刷新在
# 日志里只占**一行**。LineAssembler 负责装配，TruncatedLogWriter 负责"覆盖同一个槽位"。

_MAX_LINE_CHARS = 64 * 1024  # 单行硬上限：无 \r 也无 \n 的超长输出，强制定版防止无界增长


class LineAssembler:
    """把子进程 stdout 的**字节流**装配成日志行，按终端语义处理 \\r。

    feed()/close() 返回 ``[(行内容, 是否仍未结束), ...]``：
      • ``(text, False)`` —— 该行已由 \\n 结束（或触顶强制定版），后续不会再变。
      • ``(text, True)``  —— 该行还在被 \\r 刷新中的快照，调用方应覆盖上一次的同一行。
    调用方把二元组原样喂给写入器的 ``write(line, partial=...)`` 即可，无需自行去重。

    收 bytes 而非 str 是刻意的：定长 read(8192) 会把多字节 UTF-8 字符切在 chunk 边界上，
    逐 chunk 各自 decode 会把被切开的中文（如任务脚本抛出的中文异常回溯）变成乱码。
    这里用增量解码器跨 chunk 保留半个字符的状态，解码边界由本类统一收口。
    """

    def __init__(self) -> None:
        self._line = ""  # 当前行缓冲（已按 \r 覆盖合成）
        self._col = 0    # 光标所在列
        # errors="replace"：坏字节不抛异常（日志处理永不影响任务判定）
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    @property
    def line(self) -> str:
        """当前尚未结束的活动行内容（为空表示无）。"""
        return self._line

    def reset(self) -> None:
        """丢弃当前行状态（异常兜底用，行缓冲与光标必须一起清）。

        ⚠️ 不重置解码器：它持有的是"半个多字节字符"，与行内容无关，清掉反而制造乱码。
        """
        self._line = ""
        self._col = 0

    def feed(self, chunk: bytes) -> list[tuple[str, bool]]:
        """喂入一段 stdout 字节，返回本次产生的行事件。"""
        events: list[tuple[str, bool]] = []
        segments = self._decoder.decode(chunk).split("\n")
        for idx, seg in enumerate(segments):
            for j, piece in enumerate(seg.split("\r")):
                if j:
                    self._col = 0  # 遇到 \r：光标归零，后续字符原地覆盖
                if not piece:
                    continue
                if self._col == 0 and len(piece) >= len(self._line):
                    # 快路径：从行首整行覆盖（进度刷新的常态）——直接赋值，
                    # 避免对长行反复做 O(len) 切片拼接，杜绝"长行 × 高频刷新"的退化。
                    self._line = piece
                else:
                    self._line = (
                        self._line[: self._col] + piece + self._line[self._col + len(piece):]
                    )
                self._col += len(piece)
            if idx < len(segments) - 1:
                # 该段以 \n 结束 → 定版
                events.append((self._line, False))
                self.reset()
            elif len(self._line) > _MAX_LINE_CHARS:
                events.append((self._line, False))
                self.reset()
        if self._line:
            events.append((self._line, True))
        return events

    def close(self) -> list[tuple[str, bool]]:
        """EOF 收尾：把残留的活动行定版（写入器会覆盖其槽位，不产生重复行）。"""
        if not self._line:
            return []
        events = [(self._line, False)]
        self.reset()
        return events


class TruncatedLogWriter:
    """
    截断日志写入器

    策略：保留日志的头部 N 行和尾部 M 行，中间部分丢弃。
    适用于长时间运行的科学计算任务，避免日志文件过大。
    """

    def __init__(self, head_lines: int = 100, tail_lines: int = 2000):
        """
        Args:
            head_lines: 保留头部行数
            tail_lines: 保留尾部行数
        """
        self.head_lines = head_lines
        self.tail_lines = tail_lines
        self._head_buffer: list[str] = []
        self._tail_buffer: deque[str] = deque(maxlen=tail_lines)
        self._head_filled = False
        self._total_lines = 0
        # 内容修订号：每次 write 都自增，**含活动行的原地覆盖**。
        # 增量推送的去冗余水位必须用它而不是 _total_lines——\r 刷新进度时行数不变、
        # 内容在变，若按行数判断会在长程 MD 期间一直跳过推送，前端日志假死。
        self._writes = 0
        # 「活动行」槽位：记录上一条 partial 行落在哪个缓冲（"head"/"tail"），
        # None 表示当前没有待覆盖的活动行。记缓冲名而非索引，是为了让
        # head 恰好写满时（_head_filled 翻转）后续覆盖仍命中原槽位。
        self._live_slot: Optional[str] = None

    def write(self, line: str, partial: bool = False):
        """
        写入一行日志

        Args:
            line:    日志行（不包含换行符）
            partial: True 表示该行尚未以 \\n 结束（\\r 刷新中的活动行）。
                     连续的 partial 写入互相覆盖、只占一个槽位；随后一次
                     partial=False 的写入把该槽位定版并关闭，故整段进度刷新
                     最终只留一行、且 _total_lines 只计一次。

        ⚠️ 不变量：**单一生产者串行调用**。活动行槽位打开期间，partial=False 的语义是
        "定版当前槽位"而非"另起一行"——若有第二个生产者并发写入，它的行会覆盖掉别人的
        活动行。实际调用方只有 executor 的 drain 协程（每个 task_id 一个），天然满足。
        本类的 writelines / StreamingLogWriter 为历史遗留、全 worker 无调用方，
        若将来复用须先确认此不变量。
        """
        self._writes += 1

        if self._live_slot is not None:
            # 覆盖活动行槽位：不新增槽位、不增总行数。
            # 槽位可能已不存在（tail_lines 被配成 0 时 deque 不保留任何元素），
            # 此时静默丢弃即可——绝不能让 IndexError 冒出日志路径。
            buf = self._head_buffer if self._live_slot == "head" else self._tail_buffer
            if buf:
                buf[-1] = line
            if not partial:
                self._live_slot = None
            return

        self._total_lines += 1

        # 头部缓冲区未满时，写入头部
        if not self._head_filled:
            self._head_buffer.append(line)
            slot = "head"
            if len(self._head_buffer) >= self.head_lines:
                self._head_filled = True
        else:
            # 头部已满，写入尾部环形缓冲
            self._tail_buffer.append(line)
            slot = "tail"

        self._live_slot = slot if partial else None

    def writelines(self, lines: list[str]):
        """批量写入多行"""
        for line in lines:
            self.write(line.rstrip("\n"))

    def get_content(self) -> str:
        """
        获取截断后的完整日志内容
        
        Returns:
            截断后的日志文本
        """
        result_lines = []
        
        # 添加头部
        result_lines.extend(self._head_buffer)
        
        # 如果总行数超过头部 + 尾部，添加分隔符
        if self._total_lines > self.head_lines + self.tail_lines:
            result_lines.append(
                f"\n... [省略 {self._total_lines - self.head_lines - self.tail_lines} 行] ...\n"
            )
            # 添加尾部
            result_lines.extend(list(self._tail_buffer))
        elif self._tail_buffer:
            # 尾部缓冲非空，直接添加
            result_lines.extend(list(self._tail_buffer))
        
        return "\n".join(result_lines)

    def save_to_file(self, filepath: Path):
        """
        保存截断后的日志到文件
        
        Args:
            filepath: 目标文件路径
        """
        filepath.parent.mkdir(parents=True, exist_ok=True)
        content = self.get_content()
        filepath.write_text(content, encoding="utf-8")

    @property
    def total_lines(self) -> int:
        """获取总写入行数（包含已丢弃的；活动行的原地刷新只计一次）"""
        return self._total_lines

    @property
    def writes(self) -> int:
        """内容修订号：写入次数，含活动行原地覆盖。仅用于判断"内容是否变过"。"""
        return self._writes

    def clear(self):
        """清空缓冲区"""
        self._head_buffer.clear()
        self._tail_buffer.clear()
        self._head_filled = False
        self._total_lines = 0
        self._writes = 0
        self._live_slot = None


class StreamingLogWriter:
    """
    流式日志写入器
    
    结合本地文件流式写入和内存截断缓冲，
    支持实时日志查看和最终截断上传。
    """

    def __init__(
        self,
        task_id: str,
        work_dir: Path,
        head_lines: int = 100,
        tail_lines: int = 2000,
    ):
        """
        Args:
            task_id: 任务 ID
            work_dir: 工作目录
            head_lines: 保留头部行数
            tail_lines: 保留尾部行数
        """
        self.task_id = task_id
        self.work_dir = work_dir
        self.log_filepath = work_dir / f"{task_id}.log"
        
        self._truncated_writer = TruncatedLogWriter(
            head_lines=head_lines, tail_lines=tail_lines
        )
        self._file_handle: Optional[object] = None

    def __enter__(self):
        """上下文管理器入口"""
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self._file_handle = open(self.log_filepath, "w", encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        if self._file_handle:
            self._file_handle.close()
            self._file_handle = None

    def write(self, line: str):
        """
        写入一行日志（同时写入本地文件和内存缓冲）
        
        Args:
            line: 日志行
        """
        # 写入本地文件
        if self._file_handle:
            self._file_handle.write(line + "\n")
            self._file_handle.flush()
        
        # 写入截断缓冲
        self._truncated_writer.write(line)

    def save_truncated(self, target_path: Optional[Path] = None):
        """
        保存截断后的日志
        
        Args:
            target_path: 目标路径，默认使用工作目录下的截断文件
        """
        if target_path is None:
            target_path = self.work_dir / f"{self.task_id}.truncated.log"
        
        self._truncated_writer.save_to_file(target_path)
        return target_path

    @property
    def total_lines(self) -> int:
        """总写入行数"""
        return self._truncated_writer.total_lines


class ConcurrentLogWriter:
    """并发安全日志写入器 — Worker 通用日志接口。

    统一接口：async def write(task_id, path, line)

    特点：
      • 单实例共享：所有 GenericTaskExecutor 共用同一个 ConcurrentLogWriter
      • 按 task_id 隔离：内部为每个任务维护独立的 TruncatedLogWriter + 文件句柄
      • 同时写入本地文件和内存截断缓冲
      • 任务结束后可通过 get_truncated_content() 获取截断日志，通过 Internal API 上报
      • 任务结束后需调用 close_task() 释放文件句柄
    """

    def __init__(
        self,
        head_lines: int = 100,
        tail_lines: int = 2000,
        redact_roots: tuple[tuple[str, str], ...] = (),
        redactors: Sequence[Redactor] = (),
    ):
        """
        Args:
            head_lines:   保留头部行数
            tail_lines:   保留尾部行数
            redact_roots: 本机已知绝对路径根，由 app.build_executor 经
                          build_redact_roots() 在构造期传入（见该函数 docstring 说明为何
                          必须是构造期而非运行期登记：增量日志在任务跑到一半就开始推送）。
            redactors:    业务脱敏函数注册表（通常是 plugin.redactors）。存引用而非拷贝：
                          插件之后追加的函数也会生效。
        """
        self.head_lines = head_lines
        self.tail_lines = tail_lines
        self.redact_roots = redact_roots
        self.redactors = redactors
        self._writers: dict[str, TruncatedLogWriter] = {}
        self._handles: dict[str, object] = {}
        # 任务目录：write() 收到的 path 即 {WORK_DIR}/{task_id}/xxx.log，其父目录就是任务目录。
        # 精确值现成可得，无需从 WORK_DIR 拼 task_id 二次推导，故两个 executor 一行不改。
        self._task_dirs: dict[str, str] = {}

    async def write(self, task_id: str, path: str, line: str, partial: bool = False) -> None:
        """写入一行日志（同时写入本地文件和内存截断缓冲）。

        此方法为 async 以匹配 executor.py 的 await 调用模式，
        实际写入为同步 I/O（日志行较短，阻塞可忽略）。

        Args:
            task_id: 任务 ID，用于隔离不同任务的日志
            path:    日志文件路径（如 {WORK_DIR}/{task_id}/run.log）
            line:    日志行内容
            partial: 该行是否为 \\r 刷新中的活动行（见 TruncatedLogWriter.write）。
                     ⚠️ 只影响**截断缓冲**（前端可见、需要控量）：活动行在缓冲里只占一个
                     被反复覆盖的槽位。本地文件不做覆盖，每次调用都追加一条，因此保留了
                     活动行的演进快照（每读一段 stdout 一条），便于本机回溯进度历史。
        """
        # 首次写入时初始化该任务的 writer 和文件句柄
        if task_id not in self._writers:
            self._writers[task_id] = TruncatedLogWriter(
                head_lines=self.head_lines, tail_lines=self.tail_lines
            )
            log_path = Path(path)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            self._handles[task_id] = open(log_path, "a", encoding="utf-8")
            self._task_dirs[task_id] = str(log_path.parent)

        # 写入截断缓冲
        self._writers[task_id].write(line.rstrip("\n"), partial=partial)

        # 写入本地文件
        fh = self._handles.get(task_id)
        if fh:
            fh.write(line if line.endswith("\n") else line + "\n")
            fh.flush()

    def get_truncated_content(self, task_id: str) -> str | None:
        """获取指定任务的截断日志内容（**已脱敏，面向前端展示**）。

        统一在此收口脱敏：所有前端可见出口（流式增量推送 / 末尾步骤日志上传 /
        错误消息尾部 / 本地截断日志）都经本方法取内容，故内部接口路径、URL、上传回调
        结果在此一处剔除/遮蔽即全链生效（见 redact_for_frontend）。

        Args:
            task_id: 任务 ID

        Returns:
            脱敏后的截断日志文本，若任务不存在返回 None
        """
        writer = self._writers.get(task_id)
        if not writer:
            return None
        return redact_for_frontend(
            writer.get_content(), self._roots_for(task_id), self.redactors
        )

    def _roots_for(self, task_id: str) -> tuple[tuple[str, str], ...]:
        """该任务的脱敏根 = 任务目录（最长、必须排最前）+ 构造期登记的静态根。

        任务目录位于 WORK_DIR 之下，若让 ``[WORK_DIR]`` 先命中就再也看不出「这是任务目录」，
        故在此前置，与 build_redact_roots 的「长前缀优先」是同一条约定。
        """
        task_dir = self._task_dirs.get(task_id)
        if not task_dir:
            return self.redact_roots
        head: list[tuple[str, str]] = []
        for cand in {task_dir, str(Path(task_dir).resolve())}:
            cand = cand.rstrip("/")
            if len(cand) > 1:
                head.append((cand, "[TASK_DIR]"))
        head.sort(key=lambda kv: len(kv[0]), reverse=True)
        return tuple(head) + self.redact_roots

    def save_truncated(self, task_id: str, target_path: Path | None = None) -> Path | None:
        """保存指定任务的截断日志到文件。

        Args:
            task_id:     任务 ID
            target_path: 目标路径，默认保存到日志文件同目录下

        Returns:
            保存的文件路径，若任务不存在返回 None
        """
        writer = self._writers.get(task_id)
        if not writer:
            return None

        if target_path is None:
            # 默认保存到日志文件旁边，后缀 .truncated.log
            fh = self._handles.get(task_id)
            if fh and hasattr(fh, "name"):
                target_path = Path(fh.name).with_suffix(".truncated.log")
            else:
                return None

        writer.save_to_file(target_path)
        return target_path

    async def close_task(self, task_id: str) -> None:
        """关闭指定任务的文件句柄并清理内存缓冲。

        Args:
            task_id: 任务 ID
        """
        fh = self._handles.pop(task_id, None)
        if fh:
            fh.close()
        self._writers.pop(task_id, None)
        self._task_dirs.pop(task_id, None)

    def get_total_lines(self, task_id: str) -> int:
        """获取指定任务的总写入行数。"""
        writer = self._writers.get(task_id)
        return writer.total_lines if writer else 0

    def get_write_count(self, task_id: str) -> int:
        """获取指定任务的内容修订号（写入次数，含 \\r 活动行的原地刷新）。

        增量日志推送用它做"内容是否变过"的水位；用行数会漏掉进度刷新。
        """
        writer = self._writers.get(task_id)
        return writer.writes if writer else 0
