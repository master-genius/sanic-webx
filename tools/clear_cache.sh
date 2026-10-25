#!/usr/bin/env bash
# 清掉所有 __pycache__（升级 Python 版本、改动 import 结构后偶发的陈旧字节码问题）。
# 只动项目自己的目录：跳过 .venv / .git / node_modules，避免把第三方包的缓存也清掉再慢慢重编。
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
find . \( -name .venv -o -name .git -o -name node_modules \) -prune -o -type d -name __pycache__ -print0 \
    | xargs -0 -r rm -rf
echo "已清理 __pycache__"
