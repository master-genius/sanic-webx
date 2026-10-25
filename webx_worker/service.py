"""systemd 单元生成器 —— `python -m webx_worker service one|multi ...`

部署期运行，生成文件交使用者自行安装（本工具不碰 /etc/systemd）：

    python -m webx_worker service one   ...   单实例：<单元名>.service
    python -m webx_worker service multi ...   多实例：<单元名>@.service 模板单元 + <单元名>.target

## 设计要点（沿用线上验证过的做法）

【一 worker 一 unit】故障天然隔离：systemd 只重启它自己，不影响其他 worker。
多实例用模板单元：`systemctl enable --now <单元名>@gpu0` 时实例名 `gpu0` 代入单元里的 `%i`，
一份文件服务任意多个实例。

【一名三用：%i == channel 配置名 == 环境覆盖层名 == worker_id 后缀】
实例名 gpu0 同时决定：
  · <home>/channel_config/gpu0.py     ← `--config %i` 指定的 channel 配置（LANES / run_env）
  · <home>/channel_config/gpu0_env    ← worker 自动按约定探测的环境覆盖层（若存在）
  · worker_id = {WEBX_WORKER_PREFIX 或主机名}-gpu0
「起一个新实例 = 放一份配置 + systemctl enable --now <单元名>@<名>」。

【unit 不承载业务配置 —— 手动启动必须完全等价】
生成的 unit 里 WORKER_ARGS 只有 `--config %i`，别无他物。日志目录、工作目录、令牌等一律写进
`.env` / `<配置名>_env`，由 worker 自己读。于是手动与托管行为一致，排障时不会两套行为互相打架。

【分组 target】模板单元 `[Install]` 写 `WantedBy=<单元名>.target`，`PartOf=<单元名>.target`：
`systemctl start <单元名>.target` 一次拉起全部已 enable 的实例；stop/restart 向下传播。

【优雅停止】`TimeoutStopSec=30`：SIGTERM 到达后 worker 取消在跑任务、带令牌归还租约、
落盘失败上报队列（见 docs/worker.md「停机语义」），超时才 SIGKILL。

【PrivateTmp=true】给每个实例独立的 /tmp 命名空间——所以日志目录必须用相对路径或非 /tmp 的
绝对路径，绝不能写 /tmp/logs（日志将在宿主机上不可见、且随重启清空）。

【conda（可选）】systemd 运行时没有 conda（不读 .bashrc、PATH 无 conda），所以给了
`--conda-env` 时，conda 根目录必须在「生成时」确定并烤进启动脚本，运行时不再探测。
不给则 ExecStart 直接用 `--python` 指定的解释器（缺省为运行本工具的解释器）。
"""

from __future__ import annotations

import argparse
import getpass
import os
import pwd
import shutil
import sys
from pathlib import Path

from ._paths import PACKAGE_PARENT

DEFAULT_UNIT = "webx-worker"


# ────────────────────────── 单元名校验 / 残留检测 ──────────────────────────

def normalize_unit_name(raw: str, parser: argparse.ArgumentParser) -> str:
    """校验并归一化 --unit-name，非法值直接 parser.error() 退出。

    只做减法（剥后缀）不做加法（不擅自改写用户给的字符）：这个值会同时决定文件名
    与单元内的自引用（PartOf/WantedBy/SyslogIdentifier），静默纠正会让人以为
    生效的是别的名字。

    拦截四类必然出错、且错得不显眼的输入：
      · 带 .service / .target 后缀 → 拼成 xxx.service@.service，systemd 找不到
      · 含 `@`                     → 与模板单元的实例分隔符冲突，语义直接错乱
      · 含 `/`                     → systemd 单元名不是路径
      · 含空白 / 为空              → 文件能生成，但单元无法加载
    """
    name = raw.strip()
    for suffix in (".service", ".target"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            print(f"[service] --unit-name 已剥去后缀 {suffix}：{raw!r} → {name!r}")
            break

    if not name:
        parser.error("--unit-name 不能为空")
    if "@" in name:
        parser.error(f"--unit-name 不能含 '@'（它是模板单元的实例分隔符）: {raw!r}")
    if "/" in name:
        parser.error(f"--unit-name 不能含 '/'（systemd 单元名不是路径）: {raw!r}")
    if any(c.isspace() for c in name):
        parser.error(f"--unit-name 不能含空白字符: {raw!r}")
    return name


def warn_stale_units(out_dir: Path, keep: set[str]) -> list[str]:
    """输出目录里若残留**其他基础名**的单元文件，打印拆除指引，返回残留文件名。

    改名不是迁移：旧的 /etc/systemd/system/<旧名>.target.wants/<旧名>@gpu0.service
    符号链接不会因为新名字被 enable 而消失。两套单元同时 enable，开机后会有两个
    进程带着同一个 `--config` 起来、共用同一个工作目录，互相破坏
    （ipc.sock 抢占 / 失败上报快照互相覆盖 / recovery 扫描误删兄弟实例的任务目录）。
    故必须显式提示，不能静默生成了事。
    """
    stale = sorted(p.name for p in out_dir.glob("*.service") if p.name not in keep) + \
        sorted(p.name for p in out_dir.glob("*.target") if p.name not in keep)
    if not stale:
        return []

    old_names = sorted({n.split("@")[0].rsplit(".", 1)[0] for n in stale})
    print(f"\n[service] 输出目录检测到其他基础名的单元文件: {', '.join(stale)}")
    print("    改名不是迁移 —— 旧单元若已 enable，其符号链接仍在，")
    print("    新旧两套会同时拉起同一个 --config，共用工作目录必然互相破坏。")
    print("    如旧单元已安装，请先拆除（按实际存在的单元取用）：")
    for old in old_names:
        print(f"      sudo systemctl disable --now {old} '{old}@*'")
        print(f"      sudo rm -f /etc/systemd/system/{old}.service "
              f"/etc/systemd/system/{old}@.service /etc/systemd/system/{old}.target")
    print("      sudo systemctl daemon-reload")
    return stale


# ────────────────────────── 环境探测 ──────────────────────────

def current_user() -> str:
    """当前用户名：优先按真实 uid 反查账户库（最可靠），回退 getpass。"""
    try:
        return pwd.getpwuid(os.getuid()).pw_name
    except (KeyError, OSError):
        return getpass.getuser()


def resolve_conda_root(explicit: str | None) -> Path:
    """生成时解析 conda 安装根目录（systemd 运行时无 conda，故必须在此确定后烤入脚本）。

    探测优先级，每个候选都要求 `etc/profile.d/conda.sh` 真实存在才采纳：
      ① --conda-root 显式指定
      ② $CONDA_EXE 反推
      ③ shutil.which("conda")（天然无视 shell 函数/别名）
      ④ $CONDA_PREFIX 反推（若是 .../envs/<name> 则上溯两级取 base 根）
    全部落空 → 报错要求显式 --conda-root（在生成时暴露，远好于 systemd 运行时才崩）。
    """
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    exe = os.environ.get("CONDA_EXE")
    if exe:
        candidates.append(Path(exe).resolve().parent.parent)
    which = shutil.which("conda")
    if which:
        candidates.append(Path(which).resolve().parent.parent)
    prefix = os.environ.get("CONDA_PREFIX")
    if prefix:
        p = Path(prefix)
        candidates.append(p.parent.parent if p.parent.name == "envs" else p)

    seen: set[Path] = set()
    for c in candidates:
        c = c.resolve()
        if c in seen:
            continue
        seen.add(c)
        if (c / "etc" / "profile.d" / "conda.sh").is_file():
            return c

    tried = " / ".join(str(c) for c in candidates) or "(无候选)"
    raise SystemExit(
        "[service] 致命错误：未能确定 conda 安装根目录（其 etc/profile.d/conda.sh 不存在）。\n"
        f"  已尝试：{tried}\n"
        "  请显式指定：--conda-root /path/to/conda\n"
        "  提示：在已 conda activate 的交互 shell 里运行本命令，可自动探测。"
    )


def _needs_pythonpath() -> bool:
    """包不是装进 site-packages 的（直接从仓库目录导入）时，单元里要补 PYTHONPATH。"""
    return not any(k in PACKAGE_PARENT for k in ("site-packages", "dist-packages"))


# ────────────────────────── 模板 ──────────────────────────

LAUNCH_TEMPLATE = r'''#!/usr/bin/env bash
#
# 【本文件由 `python -m webx_worker service` 生成，请勿手改；重跑生成即可】
#
# Worker 启动包装脚本（供 systemd 调用，也可手动执行）
#
# 用法： <本脚本> --conda-env <name> [透传给 worker 的参数...]
#   例： <本脚本> --conda-env myenv --config gpu0 --debug
#
# 职责：
#   1) 解析并摘除 --conda-env（脚本自有参数，绝不透传给 worker），其余参数原样保留
#   2) source 并 activate 该 conda 环境（channel 配置里 env="conda" 的任务需要 conda 在 PATH）
#   3) cd 进 worker 主目录后 exec python -m webx_worker <其余参数>
#
# exec 关键作用：python 替换当前 shell，二者同一进程（同一 PID）= systemd MainPID。
#   stop/SIGTERM 直达 python，worker 子进程在同一 cgroup 由 systemd 一并清理。
#
set -euo pipefail

# ---------- 1. 解析参数：摘除 --conda-env，其余进 PASS_ARGS 原样透传 ----------
CONDA_ENV=""
PASS_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --conda-env)
            CONDA_ENV="${2:-}"; shift 2 ;;
        --conda-env=*)
            CONDA_ENV="${1#*=}"; shift ;;
        *)
            PASS_ARGS+=("$1"); shift ;;
    esac
done

# --conda-env 必填：缺失 / 空值直接 fail-fast，避免静默激活错误环境
if [[ -z "${CONDA_ENV}" ]]; then
    echo "[webx-worker-launch] 致命错误：缺少 --conda-env <name>（要激活的 conda 环境名）" >&2
    exit 1
fi

# ---------- 2. worker 主目录（生成时烤入，与脚本实际存放位置无关） ----------
WORKER_HOME="__WORKER_HOME__"
export WEBX_WORKER_HOME="${WORKER_HOME}"

# ---------- 3. HOME（systemd 可能不设 $HOME；conda 读 ~/.condarc、写缓存依赖它） ----------
__HOME_SETUP__

# ---------- 4. conda 根目录（生成时探测并烤入；systemd 运行时无法探测） ----------
# 允许用 systemd `Environment=CONDA_ROOT=...` 临时覆盖而无需重生成。
CONDA_ROOT="${CONDA_ROOT:-__CONDA_ROOT__}"
CONDA_SH="${CONDA_ROOT}/etc/profile.d/conda.sh"
if [[ ! -f "${CONDA_SH}" ]]; then
    echo "[webx-worker-launch] 致命错误：未找到 ${CONDA_SH}，请检查 CONDA_ROOT=${CONDA_ROOT}" >&2
    exit 1
fi

# ---------- 5. 激活 conda + 引入额外环境、进入目录、拉起 ----------
# conda 初始化（conda.sh 引用 $PS1）与 activate/deactivate 钩子，以及 --env 设备环境文件，
# 普遍未对 nounset 安全，故 source/activate 期间临时关闭 set -u，之后恢复。
set +u
# shellcheck disable=SC1090
source "${CONDA_SH}"
if ! conda activate "${CONDA_ENV}"; then
    set -u
    echo "[webx-worker-launch] 致命错误：conda activate '${CONDA_ENV}' 失败（环境不存在？）" >&2
    exit 1
fi
__ENV_SOURCE__
set -u
cd "${WORKER_HOME}"

echo "[webx-worker-launch] conda=${CONDA_ENV}  CONDA_ROOT=${CONDA_ROOT}  HOME=${HOME}  主目录=${WORKER_HOME}" >&2

# 安全展开空数组（set -u 下兼容旧 bash）
exec python -m webx_worker ${PASS_ARGS[@]+"${PASS_ARGS[@]}"}
'''

# 单实例单元。__XXX__ 由生成器替换；${CONDA_ENV} / $WORKER_ARGS 是 systemd 语法，原样保留。
# ⚠️ 单元名一律走 __UNIT__ 占位符，**绝不能**对渲染结果做全局字符串替换：
#    ExecStart 展开后既含路径也含单元名，全局替换会把可执行路径一并改坏。
SERVICE_ONE_TEMPLATE = r'''[Unit]
# 【本文件由 `python -m webx_worker service one` 生成】
Description=webx_worker (__UNIT__)
After=network.target

[Service]
Type=simple
User=__USER__
Group=__GROUP__
WorkingDirectory=__HOME__

# worker 业务配置（令牌、服务地址、工作目录、日志目录等）由 worker 自身读取
# <主目录>/.env，这里不再注入 EnvironmentFile。
Environment=WEBX_WORKER_HOME=__HOME__
__EXTRA_ENV__
# WORKER_ARGS：原样透传给 worker 的参数，留空 = 默认配置。常用参数：
#     --config <name>    指定 channel 配置（缺 .py 自动补全），如 --config gpu0
#     --log <dir>        日志落盘目录
#     --debug            打开 DEBUG 日志
#   ⚠️ 多参数必须用双引号包住「整个 VAR=值」，不是只包值：
#       正确：Environment="WORKER_ARGS=--config gpu0 --debug"
#       错误：Environment=WORKER_ARGS=--config gpu0 --debug  ← systemd 按空白拆成多个赋值
#   下方 ExecStart 用无花括号的 $WORKER_ARGS，systemd 替换后再按空白拆成多个 flag；
#   切勿改成 ${WORKER_ARGS}，那会整坨当单个参数传入。
Environment=WORKER_ARGS=
__CONDA_ENV_LINE__
ExecStart=__EXEC__ $WORKER_ARGS

Restart=on-failure
RestartSec=5

# 优雅停止：默认 KillMode=control-group，SIGTERM 发给 worker 及其子进程；超时 SIGKILL 兜底。
TimeoutStopSec=30

# 安全加固
NoNewPrivileges=true
PrivateTmp=true

StandardOutput=journal
StandardError=journal
SyslogIdentifier=__UNIT__

[Install]
WantedBy=multi-user.target
'''

# 多实例模板单元
SERVICE_MULTI_TEMPLATE = r'''[Unit]
# 【本文件由 `python -m webx_worker service multi` 生成】
# 模板单元：一份文件服务任意多个实例。实例名 %i 即 channel 配置名。
#   启动单个：sudo systemctl enable --now __UNIT__@gpu0
#   启动全部：sudo systemctl start __UNIT__.target
Description=webx_worker (__UNIT__, instance %i)
After=network.target

# PartOf 只向下传播 stop 与 restart（start 方向由 [Install] 的 WantedBy 提供）。
# 注意本行与文件名无关，是这里的显式声明才把本单元挂进该 target。
PartOf=__UNIT__.target

[Service]
Type=simple
User=__USER__
Group=__GROUP__
WorkingDirectory=__HOME__

# 业务配置由 worker 自身分两层读取，此处不注入 EnvironmentFile：
#   ① <主目录>/.env                    公共基础层（所有实例共享）
#   ② <主目录>/channel_config/%i_env   本实例覆盖层（可选，worker 按 --config 名自动探测）
# 覆盖层只需写「与公共层不同」的那几行，其余继续继承 .env。
#
# ⚠️ 多个实例绝不可共用同一个 WEBX_WORKER_WORK_DIR（ipc.sock 抢占 / 失败上报快照互相覆盖 /
#    recovery 扫描无归属过滤会删掉兄弟实例正在执行的任务目录）——
#    请在每个 %i_env 里给出**互不相同**的 WEBX_WORKER_WORK_DIR。
Environment=WEBX_WORKER_HOME=__HOME__
__EXTRA_ENV__
# WORKER_ARGS **刻意只有 --config %i**：unit 是管理编排，不是配置容器。
# 手动 `python -m webx_worker --config <名>` 与 systemctl start __UNIT__@<名> 行为完全一致。
#   ⚠️ 双引号必须包住「整个 VAR=值」而不是只包值，否则 systemd 会按空白拆成多个赋值。
Environment="WORKER_ARGS=--config %i"
__CONDA_ENV_LINE__
ExecStart=__EXEC__ $WORKER_ARGS

Restart=on-failure
RestartSec=5
TimeoutStopSec=30

NoNewPrivileges=true
# ⚠️ PrivateTmp 给每个实例独立的 /tmp 命名空间 —— 日志目录必须用相对路径或非 /tmp 的绝对路径
PrivateTmp=true

# SyslogIdentifier 带实例名，journalctl 可按实例过滤：journalctl -u __UNIT__@gpu0 -f
StandardOutput=journal
StandardError=journal
SyslogIdentifier=__UNIT__-%i

[Install]
# enable 时在 __UNIT__.target.wants/ 建符号链接 → target 获得 start 方向的控制
WantedBy=__UNIT__.target
'''

TARGET_TEMPLATE = r'''[Unit]
# 【本文件由 `python -m webx_worker service multi` 生成】
# 实例组锚点：本身不启动任何进程，只作为「所有 worker 实例」的统一开关。
#   sudo systemctl start   __UNIT__.target   # 拉起全部已 enable 的实例
#   sudo systemctl stop    __UNIT__.target   # 停止全部（经各实例的 PartOf 传播）
#   sudo systemctl restart __UNIT__.target   # 重启全部
#   systemctl list-dependencies __UNIT__.target   # 查看当前挂了哪些实例
Description=webx_worker instances (__UNIT__ group target)
After=network.target

[Install]
WantedBy=multi-user.target
'''

OVERLAY_TEMPLATE = """# ── worker 实例「{name}」环境覆盖层 ──────────────────────────
# 由 `python -m webx_worker service multi` 生成，可自由编辑。
#
# 加载时机：worker 启动时按 --config 名自动探测本文件（channel_config/{name}_env），
#   存在即叠加在 <主目录>/.env 之上。语义是「继承 + 覆盖 + 新增」——
#   本文件没写的键继续沿用 .env 的值，只需写出与公共配置**不同**的那几行。
#
# 格式同 .env：KEY=VALUE，一行一个，# 开头为注释。
# 优先级：真实环境变量 > 本文件 > .env。
#
# ⚠️ 两条边界，务必先看：
#
#   ① 路径不做 ~ 展开。请一律写绝对路径。
#
#   ② 本文件**只对 worker 主进程的配置项生效**（config.WorkerSettings 里定义的那些键）。
#      任务子进程读不到这里：线程数 / GPU 绑定这类子进程级参数请写在
#      channel_config/{name}.py 里各 channel 的 run_env。

# 【必填且每实例必须不同】任务工作目录，**绝对路径**。
# 为何不能用相对路径：本值会随 executor 注入子进程环境，而子进程 cwd 是各自的任务目录，
# 相对路径在那里会解析到错误位置。
#
# ⚠️ 绝不可把多个实例指向同一目录，也不可指向它们的**共同父目录**：
#    · 共用同一目录 → ipc.sock 被后启动者抢占、失败上报快照互相覆盖、recovery 会 rmtree 掉
#      兄弟实例正在执行的任务目录
#    · 指向父目录 → recovery 把 WORK_DIR 下每个子目录名当作 task_id 去查服务端，
#      其它实例的目录会被当成任务、查到 404 后标记 not_found、7 天后删除
WEBX_WORKER_WORK_DIR={work_dir}

# 日志目录。相对路径按主目录解析（单元 WorkingDirectory 即主目录）。
# 多实例共用同一目录是安全的：文件名以 _{{pid}} 结尾，互不覆盖。
# ⚠️ 别写成绝对的 /tmp/...：单元里 PrivateTmp=true 会给每个实例独立的 /tmp 命名空间。
WEBX_WORKER_LOG_DIR=logs


# 其余可覆盖项示例（不需要就保持注释）：
# WEBX_WORKER_ID={name}-box
"""


# ────────────────────────── 渲染（纯函数，便于测试） ──────────────────────────

def _fill(tpl: str, **kw: str) -> str:
    out = tpl
    for k, v in kw.items():
        out = out.replace(f"__{k}__", v)
    return out


def render_launch_script(home: Path, conda_root: Path, env_file: str | None,
                         home_fallback: str | None) -> str:
    if home_fallback:
        home_setup = (
            "# $HOME 为空时回退到 --home-fallback 指定值\n"
            'if [[ -z "${HOME:-}" ]]; then\n'
            f'    HOME="{Path(home_fallback).expanduser()}"\n'
            "    export HOME\n"
            "fi"
        )
    else:
        home_setup = (
            "# $HOME 为空时回退到运行用户的家目录（不依赖 $HOME 本身）\n"
            'if [[ -z "${HOME:-}" ]]; then\n'
            '    HOME="$(getent passwd "$(id -u)" | cut -d: -f6)"\n'
            "    export HOME\n"
            "fi"
        )
    if env_file:
        env_abs = str(Path(env_file).expanduser())
        env_source = (
            "# 引入设备相关环境文件（--env）\n"
            f'if [[ -f "{env_abs}" ]]; then\n'
            "    # shellcheck disable=SC1090\n"
            f'    source "{env_abs}"\n'
            "else\n"
            f'    echo "[webx-worker-launch] 致命错误：--env 文件不存在: {env_abs}" >&2\n'
            "    exit 1\n"
            "fi"
        )
    else:
        env_source = "# （未配置 --env 额外环境文件）"
    return _fill(LAUNCH_TEMPLATE, WORKER_HOME=str(home), CONDA_ROOT=str(conda_root),
                 HOME_SETUP=home_setup, ENV_SOURCE=env_source)


def render_unit(template: str, *, unit: str, user: str, group: str, home: Path,
                python: str, launch_script: Path | None, conda_env: str | None) -> str:
    """渲染单实例 / 多实例单元。

    有启动脚本（conda 模式）：`ExecStart=<脚本> --conda-env ${CONDA_ENV} $WORKER_ARGS`；
    否则直接：`ExecStart=<python> -m webx_worker run $WORKER_ARGS`。
    """
    extra = f"Environment=PYTHONPATH={PACKAGE_PARENT}" if _needs_pythonpath() else \
        "# （包已安装到 site-packages，无需 PYTHONPATH）"
    if launch_script is not None:
        conda_line = f"Environment=CONDA_ENV={conda_env}"
        exec_ = f"{launch_script} --conda-env ${{CONDA_ENV}}"
    else:
        conda_line = "# （未使用 conda：直接用下方解释器）"
        exec_ = f"{python} -m webx_worker run"
    return _fill(template, UNIT=unit, USER=user, GROUP=group, HOME=str(home),
                 EXTRA_ENV=extra, CONDA_ENV_LINE=conda_line, EXEC=exec_)


# ────────────────────────── CLI ──────────────────────────

def _common_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--unit-name", default=DEFAULT_UNIT,
                   help=f"systemd 单元基础名（默认：{DEFAULT_UNIT}）。单元名在 /etc/systemd/system "
                        "这个全局平坦命名空间里最好带项目前缀，避免与别的服务撞名")
    p.add_argument("--home", default=None,
                   help="worker 主目录（存放 .env 与 channel_config/；默认取 WEBX_WORKER_HOME，再缺省为当前目录）")
    p.add_argument("--out-dir", default=None, help="产物输出目录（默认：<主目录>/deploy）")
    p.add_argument("--python", default=sys.executable,
                   help="ExecStart 使用的解释器（默认：运行本命令的解释器；使用 conda 时无效）")
    p.add_argument("--conda-env", default=None,
                   help="要激活的 conda 环境名；给了就生成启动脚本并让单元经它启动")
    p.add_argument("--conda-root", default=None, help="conda 安装根目录（默认自动探测）")
    p.add_argument("--env", default=None, help="设备相关环境文件，启动脚本在 conda 激活后 source 引入")
    p.add_argument("--home-fallback", default=None, help="启动脚本中 $HOME 为空时的回退值")
    p.add_argument("--user", default=None, help="User=（默认：当前用户）")
    p.add_argument("--group", default=None, help="Group=（默认：与 --user 一致）")


def _resolve_common(args, parser) -> dict:
    from .config import worker_home

    if args.home:
        os.environ["WEBX_WORKER_HOME"] = str(Path(args.home).expanduser().resolve())
    home = worker_home()
    unit = normalize_unit_name(args.unit_name, parser)
    user = args.user or current_user()
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else home / "deploy"
    out_dir.mkdir(parents=True, exist_ok=True)

    launch_path = None
    if args.conda_env:
        conda_root = resolve_conda_root(args.conda_root)
        launch_path = out_dir / f"{unit}-launch.sh"
        launch_path.write_text(
            render_launch_script(home, conda_root, args.env, args.home_fallback), encoding="utf-8"
        )
        launch_path.chmod(0o755)
        print(f"[service] 已生成启动脚本: {launch_path}（conda 根: {conda_root}）")
    return dict(unit=unit, user=user, group=args.group or user, home=home, out_dir=out_dir,
                python=args.python, launch_script=launch_path, conda_env=args.conda_env)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m webx_worker service",
        description="生成 systemd 单元（部署期运行；不修改 /etc/systemd）",
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    one = sub.add_parser("one", help="单实例：<单元名>.service")
    _common_args(one)
    multi = sub.add_parser("multi", help="多实例：<单元名>@.service 模板单元 + <单元名>.target")
    _common_args(multi)
    multi.add_argument("--instances", default=None,
                       help="逗号分隔的实例名，为每个实例生成 channel_config/<名>_env 样例（已存在则跳过）")
    args = parser.parse_args(argv)
    c = _resolve_common(args, parser)
    unit, out_dir, home = c["unit"], c["out_dir"], c["home"]
    render_kw = {k: c[k] for k in ("unit", "user", "group", "home", "python",
                                    "launch_script", "conda_env")}

    if args.mode == "one":
        path = out_dir / f"{unit}.service"
        path.write_text(render_unit(SERVICE_ONE_TEMPLATE, **render_kw), encoding="utf-8")
        print(f"[service] 已生成单元: {path}")
        print(f"[service] 部署：sudo cp {path} /etc/systemd/system/{unit}.service "
              f"&& sudo systemctl daemon-reload && sudo systemctl enable --now {unit}")
        warn_stale_units(out_dir, keep={path.name, f"{unit}@.service", f"{unit}.target"})
        return

    service_path = out_dir / f"{unit}@.service"
    target_path = out_dir / f"{unit}.target"
    service_path.write_text(render_unit(SERVICE_MULTI_TEMPLATE, **render_kw), encoding="utf-8")
    target_path.write_text(_fill(TARGET_TEMPLATE, UNIT=unit), encoding="utf-8")

    names = [n.strip() for n in (args.instances or "").split(",") if n.strip()]
    overlay_dir = home / "channel_config"
    for name in names:
        overlay_dir.mkdir(parents=True, exist_ok=True)
        p = overlay_dir / f"{name}_env"
        if p.exists():
            print(f"[service] 覆盖层已存在，跳过: {p}")
            continue
        p.write_text(OVERLAY_TEMPLATE.format(name=name, work_dir=home / "tmp" / "tasks" / name),
                     encoding="utf-8")
        print(f"[service] 已生成覆盖层样例: {p}")

    print(f"[service] 已生成模板单元: {service_path}")
    print(f"[service] 已生成组 target: {target_path}")
    warn_stale_units(out_dir, keep={service_path.name, target_path.name, f"{unit}.service"})
    first = (names or ["gpu0"])[0]
    print("\n部署步骤：")
    print(f"  sudo cp {service_path} {target_path} /etc/systemd/system/")
    print("  sudo systemctl daemon-reload")
    print(f"  sudo systemctl enable {unit}.target")
    for name in names or ["gpu0"]:
        print(f"  sudo systemctl enable --now {unit}@{name}"
              f"      # 需要 channel_config/{name}.py 与（可选）channel_config/{name}_env")
    print(f"\n  journalctl -u {unit}@{first} -f      # 跟踪单个实例")
    print("\n手动测试（与 systemd 行为完全等价，因为 unit 不含任何业务配置）：")
    print(f"  cd {home} && python -m webx_worker --config {first}")
