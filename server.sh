#!/usr/bin/env bash
# ============================================================
# webx — 各角色统一启动脚本（systemd 友好）
#
#   ./server.sh [web] [选项]     Web 服务（不写角色也是它，保持向后兼容）
#   ./server.sh task  [选项]     领取服务（单进程）          python -m task_server
#   ./server.sh storage [选项]   存储服务                    python -m webx_storage
#   ./server.sh reconciler [选项] 补偿器                     python tools/reconciler.py
#   ./server.sh worker [选项]    worker                      python -m webx_worker
#
# 脚本只做两件事：① 选对解释器 ② 把角色路由到它自己的入口。
# 除 web 外的角色，其余参数**原样透传**给入口（各自的 --help 才是权威），脚本不重复定义它们的选项——
# 否则入口加一个参数这里就得跟着改，两边迟早漂移。
#
# 全部用 exec：服务进程取代本 shell，systemd 下 MainPID 即服务本体，SIGTERM 直达、无悬挂 wrapper。
# Web 用 sanic CLI 而非 `python run.py`：多 worker 由 Sanic 的 worker 管理器接管，进程崩了会自动重启，
# 也支持 --dev 的热重载。run.py 里 app 是模块级变量，两种启动方式都能用。
# ============================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- 定位 uv（systemd 极简 PATH 不含 ~/.local/bin，须自定位）---
# 优先级：已在 PATH > $UV_BIN 显式指定 > 常见安装目录。仅在用到 uv 时才需要（有 .venv 就不需要）。
_find_uv() {
    command -v uv &>/dev/null && return 0
    if [ -n "${UV_BIN:-}" ] && [ -x "$UV_BIN" ]; then
        export PATH="$(dirname "$UV_BIN"):$PATH"; return 0
    fi
    local cand
    for cand in "${HOME:-}/.local/bin" /root/.local/bin "${HOME:-}/.cargo/bin" /usr/local/bin /opt/uv/bin; do
        if [ -x "$cand/uv" ]; then export PATH="$cand:$PATH"; return 0; fi
    done
    return 1
}

# --- 解释器：$WEBX_PYTHON > 项目内 .venv > uv > 系统 python3 ---
# PYRUN 是“执行 python”的命令数组；SANIC 是“执行 sanic CLI”的命令数组。
if [ -n "${WEBX_PYTHON:-}" ]; then
    PYRUN=("$WEBX_PYTHON")          # 显式指定（conda / 系统 python / 测试用的虚拟环境），优先级最高
elif [ -x ".venv/bin/python" ]; then
    PYRUN=(".venv/bin/python")
elif _find_uv; then
    PYRUN=("uv" "run" "python")
else
    echo "⚠️  未找到 .venv 或 uv，回退到系统 python3" >&2
    PYRUN=("python3")
fi
SANIC=("${PYRUN[@]}" "-m" "sanic")

ROLES="web task storage reconciler worker"

usage() {
    cat <<EOF
用法: $0 [角色] [选项]

角色（默认 web）:
  web         Web 服务（含内置的 auth/api/admin/public/internal 路由组）
  task        领取服务（worker 长连接领任务；固定单进程）
  storage     独立存储服务
  reconciler  补偿器（租约回收 / 准入 / 巡检）
  worker      算力 worker

选项:
  web 角色:
    --host <地址>       监听地址        (默认: 配置里的 SERVER_HOST)
    --port <端口>       监听端口        (默认: 配置里的 SERVER_PORT)
    --workers <数量>    worker 数量     (默认: 4)
    --modules <列表>    装配哪些业务模块，逗号分隔，如 app 或 app,appx
                        (默认: 环境变量 WEBX_MODULES，再默认 app)
    --builtin <列表>    只挂这些内置路由组，逗号分隔(auth,api,admin,public,internal)
                        (默认: 环境变量 WEBX_BUILTIN；不配则全挂)
    --no-builtin        一个内置路由组都不挂（优先级最高）
    --debug             开启 debug（详细日志 + 错误堆栈）
    --dev               开启 dev（含热重载，隐含 debug；自动带 --no-repl）
  其它角色:
    选项原样透传给入口，用 '$0 <角色> --help' 查看。

示例:
  $0                                  # Web 生产默认：4 workers
  $0 web --dev --workers 1            # 本地开发：热重载、单进程好断点
  $0 web --port 9000 --modules app,appx
  $0 web --no-builtin                 # 纯业务服务，零内置端点
  $0 task                             # 领取服务（监听 TASK_SERVER_HOST:TASK_SERVER_PORT）
  $0 storage --workers 4 --port 9010  # 存储服务
  $0 reconciler                       # 补偿器
  $0 worker --home /data/worker --config gpu0

环境变量: WEBX_PYTHON=<解释器路径> 显式指定 python（默认 .venv > uv > python3）。
各角色的部署顺序、端口、反代、验证命令见 deploy/README.md。
EOF
}

# --- 角色：首个参数是已知角色名就取走；以 - 开头或为空 = web（向后兼容）---
ROLE="web"
if [ $# -gt 0 ]; then
    case "$1" in
        -h|--help|help) usage; exit 0 ;;
        web|task|storage|reconciler|worker) ROLE="$1"; shift ;;
        -*) ;;  # 选项直接给 web
        *) echo "❌ 未知角色: $1（可选: $ROLES）" >&2; usage >&2; exit 1 ;;
    esac
fi

case "$ROLE" in
    task)       exec "${PYRUN[@]}" -m task_server "$@" ;;
    storage)    exec "${PYRUN[@]}" -m webx_storage "$@" ;;
    reconciler) exec "${PYRUN[@]}" tools/reconciler.py "$@" ;;
    worker)     exec "${PYRUN[@]}" -m webx_worker "$@" ;;
esac

# ============================ web ============================
HOST=""
PORT=""
WORKERS=4
DEBUG=false
DEV=false
MODULES=""
BUILTIN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)       HOST="$2"; shift 2 ;;
        --port)       PORT="$2"; shift 2 ;;
        --workers)    WORKERS="$2"; shift 2 ;;
        --modules)    MODULES="$2"; shift 2 ;;
        --builtin)    BUILTIN="$2"; shift 2 ;;
        --no-builtin) BUILTIN="false"; shift ;;
        --debug)      DEBUG=true; shift ;;
        --dev)        DEV=true; shift ;;
        -h|--help)    usage; exit 0 ;;
        *) echo "❌ 未知参数: $1" >&2; usage >&2; exit 1 ;;
    esac
done

# sanic CLI 不读我们的配置：不显式传 --host/--port 就是 Sanic 自己的默认（127.0.0.1:8000），
# 与 .env 的 SERVER_HOST / SERVER_PORT 毫无关系。所以这里先把配置里的值解出来再传给它。
# 命令行显式给的值优先；解析失败只告警（不阻止启动），此时落回 Sanic 默认并在下方横幅里如实显示。
if [ -z "$HOST" ] || [ -z "$PORT" ]; then
    if _resolved="$("${PYRUN[@]}" -c "
from pathlib import Path
from webx.paths import configure_root
configure_root(Path('.').resolve())
from app.config import settings
print(settings.SERVER_HOST, settings.SERVER_PORT)
" 2>/dev/null)" && [ -n "$_resolved" ]; then
        read -r _H _P <<<"$_resolved"
        [ -z "$HOST" ] && HOST="$_H"
        [ -z "$PORT" ] && PORT="$_P"
    else
        echo "⚠️  未能从配置读取 SERVER_HOST/SERVER_PORT，将使用 Sanic 默认（127.0.0.1:8000）；可用 --host/--port 显式指定" >&2
    fi
fi

# run:app —— run.py 的模块级 app 变量。sanic CLI 会 import 这个模块，
# 模块级的 create_app() 随之执行；__main__ 守卫使 app.run() 不会被重复调用。
SANIC_ARGS=("run:app" "--workers=$WORKERS")
[ -n "$HOST" ] && SANIC_ARGS+=("--host=$HOST")
[ -n "$PORT" ] && SANIC_ARGS+=("--port=$PORT")
[ "$DEBUG" = true ] && SANIC_ARGS+=("--debug")
# --dev = debug + 热重载 + REPL。REPL 的 input() 在非 TTY（systemd / 管道）下抛 EOFError，故显式关掉；
# --dev 本身隐含 debug，两个都给会重复，所以互斥处理。
[ "$DEV" = true ] && SANIC_ARGS+=("--dev" "--no-repl")

# ⚠️ 模块列表只能走环境变量：sanic CLI **严格拒绝未知参数**（实测
# `sanic: error: unrecognized arguments: --modules=app`），不是忽略。
# --debug/--dev 则是 Sanic 自己就认识的，原样透传；run.py 的 parse_known_args
# 同时也能读到它们，据此 apply_flags。
[ -n "$MODULES" ] && export WEBX_MODULES="$MODULES"
[ -n "$BUILTIN" ] && export WEBX_BUILTIN="$BUILTIN"

echo "=============================================="
echo "  webx 启动中"
echo "  地址   : ${HOST:-127.0.0.1}:${PORT:-8000}"
echo "  workers: $WORKERS"
echo "  业务模块: ${MODULES:-<WEBX_MODULES 或默认 app>}"
echo "  内置路由: ${BUILTIN:-<WEBX_BUILTIN 或全挂>}"
echo "  debug=$DEBUG  dev=$DEV"
echo "=============================================="

exec "${SANIC[@]}" "${SANIC_ARGS[@]}"
