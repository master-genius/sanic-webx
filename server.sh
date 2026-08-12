#!/usr/bin/env bash
# ============================================================
# webx — 服务启动脚本
#
# 用 sanic CLI 而非 `python run.py`：多 worker 由 Sanic 的 worker 管理器接管，
# 进程崩了会自动重启，也支持 --dev 的热重载。run.py 里 app 是模块级变量，
# 两种启动方式都能用（``if __name__ == "__main__"`` 守卫让 sanic CLI 导入它时
# 不会重复调用 app.run）。
# ============================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# --- 解释器：优先项目内 .venv，其次 uv，最后系统 python3 ---
if [ -x ".venv/bin/python" ]; then
    RUNNER=(".venv/bin/python" "-m" "sanic")
elif command -v uv &>/dev/null; then
    RUNNER=("uv" "run" "sanic")
else
    echo "⚠️  未找到 .venv 或 uv，回退到系统 python3"
    RUNNER=("python3" "-m" "sanic")
fi

# --- 默认参数（未指定时由 .env 的 SERVER_HOST / SERVER_PORT 决定）---
HOST=""
PORT=""
WORKERS=4
DEBUG=false
DEV=false
MODULES=""
BUILTIN=""

usage() {
    cat <<EOF
用法: $0 [选项]

选项:
  --host <地址>       监听地址        (默认: .env 的 SERVER_HOST)
  --port <端口>       监听端口        (默认: .env 的 SERVER_PORT)
  --workers <数量>    worker 数量     (默认: $WORKERS)
  --modules <列表>    装配哪些业务模块，逗号分隔，如 app 或 app,appx
                      (默认: 环境变量 WEBX_MODULES，再默认 app)
  --builtin <列表>    只挂这些内置路由组，逗号分隔(auth,api,admin,public)
                      (默认: 环境变量 WEBX_BUILTIN；不配则全挂)
  --no-builtin        一个内置路由组都不挂（优先级最高）
  --debug             开启 debug（详细日志 + 错误堆栈）
  --dev               开启 dev（含热重载，隐含 debug）
  -h, --help          显示本帮助

示例:
  $0                                  # 生产默认：4 workers
  $0 --dev --workers 1                # 本地开发：热重载、单进程好断点
  $0 --port 9000 --modules app,appx   # 只在这个进程装配两个业务模块
  $0 --no-builtin                     # 纯业务服务，零内置端点
  $0 --builtin auth,api               # 只要登录与用户/文件，不要 admin 后台
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)    HOST="$2"; shift 2 ;;
        --port)    PORT="$2"; shift 2 ;;
        --workers) WORKERS="$2"; shift 2 ;;
        --modules) MODULES="$2"; shift 2 ;;
        --builtin)    BUILTIN="$2"; shift 2 ;;
        --no-builtin) BUILTIN="false"; shift ;;
        --debug)   DEBUG=true; shift ;;
        --dev)     DEV=true; shift ;;
        -h|--help) usage ;;
        *) echo "❌ 未知参数: $1"; usage ;;
    esac
done

# --- 组装 sanic 参数 ---
# run:app —— run.py 的模块级 app 变量。sanic CLI 会 import 这个模块，
# 模块级的 create_app() 随之执行；__main__ 守卫使 app.run() 不会被重复调用。
SANIC_ARGS=("run:app" "--workers=$WORKERS")

[ -n "$HOST" ] && SANIC_ARGS+=("--host=$HOST")
[ -n "$PORT" ] && SANIC_ARGS+=("--port=$PORT")
[ "$DEBUG" = true ] && SANIC_ARGS+=("--debug")
# --dev 隐含 debug + 热重载；两个都给会重复，故互斥处理
[ "$DEV" = true ] && SANIC_ARGS+=("--dev")

# ⚠️ 模块列表只能走环境变量：sanic CLI **严格拒绝未知参数**（实测
# `sanic: error: unrecognized arguments: --modules=app`），不是忽略。
# --debug/--dev 则是 Sanic 自己就认识的，原样透传；run.py 的 parse_known_args
# 同时也能读到它们，据此 apply_flags。
[ -n "$MODULES" ] && export WEBX_MODULES="$MODULES"
[ -n "$BUILTIN" ] && export WEBX_BUILTIN="$BUILTIN"

echo "=============================================="
echo "  webx 启动中"
echo "  地址   : ${HOST:-<.env>}:${PORT:-<.env>}"
echo "  workers: $WORKERS"
echo "  业务模块: ${MODULES:-<.env WEBX_MODULES>}"
echo "  内置路由: ${BUILTIN:-<.env WEBX_BUILTIN>}"
echo "  debug=$DEBUG  dev=$DEV"
echo "=============================================="

exec "${RUNNER[@]}" "${SANIC_ARGS[@]}"
