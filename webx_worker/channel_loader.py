"""Channel 配置文件加载器。

职责：
  • 解析配置文件路径（--config 或默认路径）
  • 检测配置文件是否存在，不存在则写出一份最小模板
  • 动态加载 channel 配置文件，返回 CHANNELS / LANES / CONDA_ROOT / PLUGIN / SCRIPT_ROOT
  • 根据 channel 的 env 配置构造执行命令前缀

## channel 配置文件长什么样

它是一个普通的 Python 文件（被 exec 加载），约定这些名字：

    CHANNELS = {                      # 必填。键 = task_type = 本 worker 的能力声明
        "demo": {
            "script": "demo_task.py", # 相对 SCRIPT_ROOT（缺省 = 本配置文件所在目录），或绝对路径
            "env": "",                # "conda" | "uv" | ""（直接执行）
            "env_name": "",           # conda 环境名 / uv 的 python 路径
            "executor": "python",     # env="" 时的解释器
            "run_env": {"K": "V"},    # 注入子进程的环境变量（机器级设备绑定等）
        },
    }
    LANES = [["a", "b"], ["c"]]       # 可选。空/缺省 = 所有 channel 归入一条 Lane
    CONDA_ROOT = "/opt/conda"         # 可选。缺省自动探测
    SCRIPT_ROOT = "/path/to/scripts"  # 可选。相对脚本路径的解析根
    PLUGIN = MyPlugin()               # 可选。TaskKindPlugin 子类实例（见 plugin.py）

## 目录约定

配置、`.env` 都锚定到「worker 主目录」（`WEBX_WORKER_HOME`，缺省 = 当前目录），
**不是包源码目录**——包可能装在 site-packages 里：

    <home>/.env
    <home>/channel_config/default.py       ← 缺省配置
    <home>/channel_config/gpu0.py          ← --config gpu0
    <home>/channel_config/gpu0_env         ← gpu0 的实例覆盖层（可选）
"""

import importlib.util
import os
import shutil
from pathlib import Path

from .config import worker_home
from .plugin import TaskKindPlugin

# ── 常量 ──────────────────────────────────────────────────────────
# 同一个主目录下可放多个配置文件（如 gpu0.py / gpu1.py），配合各自的 run_env
# 绑定不同设备，无需克隆目录即可启动多个 worker。
DEFAULT_CONFIG_DIR = "channel_config"
DEFAULT_CONFIG_NAME = "default.py"


# ── 公开 API ─────────────────────────────────────────────────────

def resolve_config_path(custom_path: str | None = None, home: Path | None = None) -> Path:
    """解析配置文件路径。

    优先级：
      1. 未指定 --config            → <home>/channel_config/default.py
      2. --config 为裸文件名         → <home>/channel_config/<name>（缺 .py 自动补）
      3. --config 带路径 / 绝对路径  → 直接使用该路径
    """
    config_dir = (home or worker_home()) / DEFAULT_CONFIG_DIR

    if not custom_path:
        return config_dir / DEFAULT_CONFIG_NAME

    # 裸文件名（不含目录分量且非绝对路径）→ 到 channel_config/ 目录下查找
    p = Path(custom_path)
    if p.name == custom_path and not p.is_absolute():
        name = custom_path if custom_path.endswith(".py") else f"{custom_path}.py"
        return (config_dir / name).resolve()

    # 带路径或绝对路径 → 直接使用
    return p.resolve()


def ensure_config_exists(config_path: Path) -> bool:
    """检测配置文件是否存在，不存在则写出最小模板。

    Returns:
        True:  配置已存在（可直接加载）
        False: 配置刚创建，需要用户编辑后重新启动
    """
    if config_path.exists():
        return True

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(_load_template(), encoding="utf-8")

    print(f"\n{'=' * 60}")
    print(f"  Channel 配置文件已创建: {config_path}")
    print("  请编辑此文件，声明本 worker 能跑的任务类型（CHANNELS）后，")
    print("  重新启动 Worker。")
    print(f"{'=' * 60}\n")
    return False


def load_channel_config(config_path: Path) -> dict:
    """动态加载 channel 配置文件。

    Returns:
        {
            "CHANNELS": {...},
            "CONDA_ROOT": "...",
            "LANES": [...],            # 可选，默认空数组（所有 channel 归入一条 Lane）
            "PLUGIN": TaskKindPlugin,  # 可选，默认中性插件
            "SCRIPT_ROOT": Path,       # 相对脚本路径的解析根
        }
    """
    spec = importlib.util.spec_from_file_location("channel_config", config_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载配置文件: {config_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    channels = getattr(module, "CHANNELS", {})
    if not channels:
        raise ValueError(f"配置文件中未定义 CHANNELS: {config_path}")

    conda_root = getattr(module, "CONDA_ROOT", None) or _detect_conda_root()
    lanes = getattr(module, "LANES", [])  # 可选，默认空数组

    plugin = getattr(module, "PLUGIN", None)
    if plugin is None:
        plugin = TaskKindPlugin()
    elif not isinstance(plugin, TaskKindPlugin):
        raise TypeError(f"PLUGIN 必须是 TaskKindPlugin 的实例: {config_path}")

    script_root = getattr(module, "SCRIPT_ROOT", None) or config_path.parent

    return {
        "CHANNELS": channels,
        "CONDA_ROOT": conda_root,
        "LANES": lanes,
        "PLUGIN": plugin,
        "SCRIPT_ROOT": Path(script_root).expanduser().resolve(),
    }


def build_cmd_prefix(channel_config: dict, conda_root: str) -> list[str]:
    """构建执行命令前缀（不含脚本路径）。

    根据 env 字段决定执行方式：
      env="conda" → ["conda", "run", "--no-capture-output", "-n", env_name]
      env="uv"    → ["uv", "run", "--python", env_name] 或 ["uv", "run"]
      env=""      → [executor]  （executor 字段，默认 "python"）

    Args:
        channel_config: 单个 channel 的配置字典
        conda_root:     已不再使用，保留为向后兼容参数

    Returns:
        命令前缀列表，调用方追加脚本路径即可
    """
    env_type = channel_config.get("env", "")

    if env_type == "conda":
        env_name = channel_config.get("env_name")
        if not env_name:
            raise ValueError("env='conda' 需要配置 'env_name' 字段")
        return ["conda", "run", "--no-capture-output", "-n", env_name]

    elif env_type == "uv":
        env_name = channel_config.get("env_name", "")
        if env_name:
            return ["uv", "run", "--python", str(Path(env_name).resolve())]
        return ["uv", "run"]

    else:
        # 直接执行：当前环境已进入目标 Python 环境，或通过 executor 显式指定
        executor = channel_config.get("executor", "python")
        return [executor]


# ── 内部函数 ─────────────────────────────────────────────────────

_TEMPLATE = '''\
# webx_worker — Channel 配置文件（首次启动自动生成，请按需编辑）
#
# CHANNELS 的键就是 task_type —— 它同时是本 worker 的「能力声明」：
# 每次领取请求都会带上「我这个空槽能跑哪些 task_type」，服务端不存能力注册表。
#
# 每个 channel 的字段：
#   script     任务脚本。相对路径按 SCRIPT_ROOT（缺省 = 本文件所在目录）解析，也可写绝对路径
#   env        "conda" → conda run -n <env_name> python <script>
#              "uv"    → uv run [--python <env_name>] python <script>
#              ""      → 直接执行（用 executor 指定的解释器，缺省 python）
#   env_name   conda 环境名 / uv 的 python 路径
#   executor   env="" 时的解释器路径
#   run_env    注入子进程的环境变量（字符串）——GPU/线程绑定这类「机器属性」放这里，
#              不要写进任务参数，也不要写死进代码
#
# 任务超时由任务服务下发（timeout_seconds），不在此处配置。
#
# 取消注释并改成你自己的任务：
CHANNELS = {
    # "demo": {
    #     "script": "demo_task.py",
    #     "env": "",
    #     "executor": "python",
    # },
}

# Lane 分组（可选）。每个子数组是一条 Lane = 一个槽位：组内任务串行，组间并行。
# 不配置或空数组 = 所有 channel 归入【一条】Lane（单 Lane 串行）。
# 并发的扩展轴是【进程】（多起 worker 实例），不是同类型多槽。
#   LANES = [["gpu_job", "gpu_job2"], ["cpu_job"]]
LANES = []

# 业务接缝（可选）：见 webx_worker/plugin.py
#   from webx_worker.plugin import TaskKindPlugin
#   PLUGIN = TaskKindPlugin()
'''


def _load_template() -> str:
    return _TEMPLATE


def _detect_conda_root() -> str:
    """自动检测 conda 根路径。

    优先级：
      1. 环境变量 CONDA_PREFIX（且不在 envs 子目录中）
      2. 环境变量 CONDA_ROOT
      3. PATH 中 conda 可执行文件所在目录的上一级
      4. 默认 /opt/conda
    """
    # 1. CONDA_PREFIX
    prefix = os.environ.get("CONDA_PREFIX", "")
    if prefix and "envs" not in Path(prefix).parts:
        return prefix

    # 2. CONDA_ROOT
    root = os.environ.get("CONDA_ROOT", "")
    if root:
        return root

    # 3. 从 PATH 中的 conda 推导
    conda_bin = shutil.which("conda")
    if conda_bin:
        return str(Path(conda_bin).parent.parent)

    # 4. 默认值
    return "/opt/conda"
