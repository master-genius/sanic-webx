"""`python -m webx_worker doctor` —— 部署自检：对**真实服务**验证这台机器能不能当 worker。

在把 worker 交给 systemd 之前跑一遍，把「配置对不对、网通不通、令牌有没有效」这类只会在
任务投递之后才暴露的问题提前到现在。**不会领取任何任务、不会改任何数据**：

  检查项                         判据
  ─────────────────────────────  ──────────────────────────────────────────────────────
  channel 配置                   能加载；CHANNELS 非空；PLUGIN 类型对；列出 Lane 与能力声明
  channel 脚本 / 解释器           脚本存在；conda / uv / 指定解释器在 PATH 上
  工作目录                        能创建、可写
  令牌                            已配置（空令牌 = 服务端拒绝一切请求）
  领取通道                        对 POST /task/leases 发一个【空 slots】请求：
                                   400 = 可达且鉴权通过（服务端按预期因 slots 非法而拒）
                                   401 = 令牌错；404 = 该地址没有这条路由（反代未转发）
                                  空 slots 永远领不到任何任务，所以这个探测是安全的。
  回调通道                        GET /internal/tasks/<哨兵ID>：404（或 200）= 可达且鉴权通过；401 = 令牌错
  存储服务（可选 / 已配置时）       GET /internal/uploads/<哨兵ID>：404 = 可达且鉴权通过；401 = 令牌错

退出码：0 = 全部通过（可有警告）；1 = 有失败项；2 = 参数/加载错误。
"""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path

import httpx

from . import envnames as E
from .channel_loader import build_cmd_prefix, load_channel_config, resolve_config_path
from .config import (
    apply_env_overlay,
    default_overlay_for,
    derive_worker_id,
    load_base_env,
    refresh_settings,
    worker_home,
    worker_settings,
)
from .lane_manager import LaneManager
from .link_health import PROBE_TASK_ID, classify_exception, classify_status

OK, WARN, FAIL = "ok", "warn", "fail"
_MARK = {OK: "[通过]", WARN: "[警告]", FAIL: "[失败]"}


class Report:
    def __init__(self) -> None:
        self.items: list[tuple[str, str, str]] = []

    def add(self, level: str, name: str, detail: str = "") -> None:
        self.items.append((level, name, detail))
        print(f"  {_MARK[level]} {name}" + (f" —— {detail}" if detail else ""), flush=True)

    @property
    def failed(self) -> bool:
        return any(lv == FAIL for lv, _, _ in self.items)

    def summary(self) -> str:
        n = {lv: sum(1 for x, _, _ in self.items if x == lv) for lv in (OK, WARN, FAIL)}
        return f"通过 {n[OK]} 项，警告 {n[WARN]} 项，失败 {n[FAIL]} 项"


# ───────────────────────── 单项检查 ─────────────────────────

def _classify_conn_error(exc: Exception) -> tuple[str, str]:
    kind = classify_exception(exc)
    if kind == "tls":
        return "TLS/证书问题", "检查服务端是否用了 fullchain 证书、域名是否与地址一致、本机 CA 是否过旧"
    return "连接失败", "检查服务是否已启动、地址（协议/域名/端口）是否正确、防火墙与反代 upstream"


def check_lease_channel(rep: Report, client: httpx.Client, base: str) -> None:
    """对 /task/leases 发空 slots：不可能领到任务，却足以区分「路由/鉴权/可达」三类故障。"""
    url = f"{base.rstrip('/')}/task/leases"
    try:
        r = client.post(
            url,
            headers={E.HEADER_INTERNAL_TOKEN: worker_settings.INTERNAL_TOKEN,
                     E.HEADER_WORKER_ID: worker_settings.WORKER_ID},
            json={"worker_id": worker_settings.WORKER_ID, "slots": []},
        )
    except Exception as e:  # noqa: BLE001
        what, hint = _classify_conn_error(e)
        rep.add(FAIL, "领取通道", f"{what}: {type(e).__name__}: {e} ← {url}（{hint}）")
        return
    c = r.status_code
    if c == 400:
        rep.add(OK, "领取通道", f"可达且令牌有效（空 slots 被服务端按预期以 400 拒绝）← {url}")
    elif c == 401:
        rep.add(FAIL, "领取通道", f"401：令牌错误 —— 核对 {E.ENV_INTERNAL_TOKEN} 与服务端配置、反代是否透传 "
                                f"X-Internal-Token ← {url}")
    elif c == 404:
        rep.add(FAIL, "领取通道", f"404：该地址上没有 /task/leases —— 多半是反代未转发，或 "
                                f"WEBX_WORKER_TASK_SERVER_URL 为空而回落到了 {base} ← {url}")
    elif c == 200:
        rep.add(WARN, "领取通道", "空 slots 竟被接受（200）：服务端校验比协议宽松，请核对协议实现")
    else:
        rep.add(FAIL, "领取通道", f"意外状态码 {c}（期望 400）← {url}")


def check_callback_channel(rep: Report, client: httpx.Client, base: str) -> None:
    url = f"{base.rstrip('/')}/internal/tasks/{PROBE_TASK_ID}"
    try:
        r = client.get(url, headers={E.HEADER_INTERNAL_TOKEN: worker_settings.INTERNAL_TOKEN})
    except Exception as e:  # noqa: BLE001
        what, hint = _classify_conn_error(e)
        rep.add(FAIL, "回调通道", f"{what}: {type(e).__name__}: {e} ← {url}（{hint}）")
        return
    c = r.status_code
    if c in (404, 200):
        rep.add(OK, "回调通道", f"可达且令牌有效（哨兵任务回 {c}）← {url}")
    elif classify_status(c):
        rep.add(FAIL, "回调通道", f"{c}：令牌错误 ← {url}")
    else:
        rep.add(FAIL, "回调通道", f"意外状态码 {c}（期望 404）← {url}")


def check_storage(rep: Report, client: httpx.Client) -> None:
    base = (worker_settings.STORAGE_INTERNAL_URL.strip() or worker_settings.STORAGE_URL.strip())
    if not base:
        rep.add(WARN, "存储服务", f"未配置（{E.ENV_STORAGE_INTERNAL_URL} / {E.ENV_STORAGE_URL}）："
                                 "产物与步骤日志将无处可传，uploader 不会拉起")
        return
    token = worker_settings.STORAGE_INTERNAL_TOKEN.strip() or worker_settings.INTERNAL_TOKEN
    url = f"{base.rstrip('/')}/internal/uploads/{PROBE_TASK_ID}"
    try:
        r = client.get(url, headers={E.HEADER_INTERNAL_TOKEN: token})
    except Exception as e:  # noqa: BLE001
        what, hint = _classify_conn_error(e)
        rep.add(FAIL, "存储服务", f"{what}: {type(e).__name__}: {e} ← {url}（{hint}）")
        return
    c = r.status_code
    if c in (404, 200):
        rep.add(OK, "存储服务", f"可达且令牌有效（哨兵会话回 {c}）← {url}")
    elif c == 401:
        rep.add(FAIL, "存储服务", f"401：令牌错误 —— 核对 {E.ENV_STORAGE_INTERNAL_TOKEN}（未配则回退主令牌）← {url}")
    else:
        rep.add(FAIL, "存储服务", f"意外状态码 {c}（期望 404）；反代是否转发了 /internal ← {url}")


def check_workdir(rep: Report) -> None:
    wd = Path(worker_settings.WORK_DIR).expanduser()
    try:
        wd.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=wd, prefix=".doctor-", delete=True) as f:
            f.write(b"ok")
        rep.add(OK, "工作目录", f"可写 ← {wd}")
    except OSError as e:
        rep.add(FAIL, "工作目录", f"不可写: {e} ← {wd}")


def check_channels(rep: Report, cfg: dict) -> None:
    channels, lanes = cfg["CHANNELS"], cfg["LANES"]
    lm = LaneManager(lanes, channel_types=channels.keys())
    for i in range(lm.num_lanes):
        rep.add(OK, f"Lane {i}", f"能力声明 = {lm.types_of(i)}（一条 Lane 同时最多跑一个任务）")
    for t in channels:
        if t not in lm.task_to_lane:
            rep.add(WARN, f"channel {t}", "已声明但不在任何 Lane 里，永远领不到该类型的任务")
    root = Path(cfg["SCRIPT_ROOT"])
    for t, c in channels.items():
        script = c.get("script")
        if not script:
            rep.add(FAIL, f"channel {t}", "缺少 script 字段")
            continue
        sp = root / script
        if not sp.exists():
            rep.add(FAIL, f"channel {t}", f"脚本不存在: {sp}")
            continue
        try:
            prefix = build_cmd_prefix(c, cfg["CONDA_ROOT"])
        except ValueError as e:
            rep.add(FAIL, f"channel {t}", str(e))
            continue
        if script.endswith(".sh"):
            exe = "bash"
        elif script.endswith(".js"):
            exe = "node"
        else:
            exe = prefix[0]
        if shutil.which(exe) or (os.path.isabs(exe) and os.access(exe, os.X_OK)):
            rep.add(OK, f"channel {t}", f"脚本存在，解释器可用（{exe}）")
        else:
            rep.add(FAIL, f"channel {t}", f"解释器不在 PATH 上: {exe}")


# ───────────────────────── 入口 ─────────────────────────

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m webx_worker doctor", description="部署自检（对真实服务）")
    ap.add_argument("--home", default=None, help="worker 主目录（缺省取 WEBX_WORKER_HOME，再缺省为当前目录）")
    ap.add_argument("--config", default=None, help="channel 配置（同 run）")
    ap.add_argument("--ext-env", default=None, help="实例环境覆盖层文件（同 run）")
    ap.add_argument("--work-dir", default=None, help="覆盖工作目录")
    ap.add_argument("--timeout", type=float, default=10.0, help="单次探测超时秒数（默认 10）")
    ap.add_argument("--no-storage", action="store_true", help="跳过存储服务检查")
    args = ap.parse_args(argv)

    if args.home:
        os.environ["WEBX_WORKER_HOME"] = str(Path(args.home).expanduser().resolve())
    home = worker_home()
    refresh_settings()          # 按当前环境变量重建配置（import 之后环境可能变过）
    rep = Report()
    print(f"webx_worker doctor —— 主目录 {home}\n")

    # —— 配置加载（与 run 同序：.env → 覆盖层 → worker_id → 命令行覆盖）——
    base_loaded = load_base_env(home / ".env")
    rep.add(OK if base_loaded else WARN, ".env", str(base_loaded) if base_loaded else
            f"未找到 {home / '.env'}（只用真实环境变量与默认值）")
    config_path = resolve_config_path(args.config, home)
    overlay = apply_env_overlay(args.ext_env) if args.ext_env else \
        apply_env_overlay(default_overlay_for(config_path))
    if args.ext_env and overlay is None:
        rep.add(FAIL, "环境覆盖层", f"--ext-env 指定的文件不存在: {args.ext_env}")
    elif overlay:
        rep.add(OK, "环境覆盖层", str(overlay))
    wid = derive_worker_id(config_path.stem)
    rep.add(OK, "worker_id", wid)
    if args.work_dir:
        worker_settings.WORK_DIR = str(Path(args.work_dir).expanduser().resolve())

    cfg = None
    if not config_path.exists():
        rep.add(FAIL, "channel 配置", f"不存在: {config_path}（首次运行 worker 会写出模板）")
    else:
        try:
            cfg = load_channel_config(config_path)
            rep.add(OK, "channel 配置", f"{config_path}（{len(cfg['CHANNELS'])} 个 channel）")
        except Exception as e:  # noqa: BLE001
            rep.add(FAIL, "channel 配置", f"加载失败: {type(e).__name__}: {e}")
    if cfg:
        check_channels(rep, cfg)

    check_workdir(rep)

    if not worker_settings.INTERNAL_TOKEN.strip():
        rep.add(FAIL, "令牌", f"{E.ENV_INTERNAL_TOKEN} 未配置（空令牌 = 服务端拒绝一切请求）")
    else:
        rep.add(OK, "令牌", "已配置")

    lease_base = worker_settings.TASK_SERVER_URL.strip() or worker_settings.WEB_BASE_URL
    with httpx.Client(timeout=args.timeout) as client:
        check_lease_channel(rep, client, lease_base)
        check_callback_channel(rep, client, worker_settings.WEB_BASE_URL)
        if not args.no_storage:
            check_storage(rep, client)

    print(f"\n{rep.summary()}")
    return 1 if rep.failed else 0
