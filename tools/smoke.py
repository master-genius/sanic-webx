"""上线冒烟：从用户视角把整条链路走一遍，给“系统是不是真的通了”一个明确的答案。

    python tools/smoke.py --web https://api.example.com --login alice:secret
    python tools/smoke.py --web http://127.0.0.1:9009 --token <用户令牌> --storage http://127.0.0.1:9010
    python tools/smoke.py --web ... --token ... --task-type demo --params '{"seconds": 2}'

做的事：
  1. ``GET /health``                  Web 可达
  2. 取得用户令牌                      ``--token`` 直接用，或 ``--login 用户名:密码`` 走 ``POST /auth/login``
  3. ``POST /api/tasks``（submit=true）  创建并提交一个任务
  4. 轮询到终态，打印每次状态变化      证明 worker 真的领到了、跑了、回调了
  5. 完成后核对产物                   ``result_path`` 里每个地址都要能下载（相对地址按 ``--storage`` 补全）
  6. 清理                            默认删掉这个任务（``--keep`` 保留）

退出码：0 = 全链路通；1 = 任务失败 / 超时 / 产物不可下载；2 = 前置步骤失败（连不上、登录失败）。

它**会在线上创建一个真实任务**并占用一个 worker 槽位，所以默认任务类型是 worker 示例里的 ``demo``
（睡一秒、写两个文件），不碰业务。要验证业务类型请显式给 ``--task-type`` 与 ``--params``。
只依赖 httpx，可单独拷到任何能访问服务的机器上运行。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

import httpx

TERMINAL = {"completed", "failed", "cancelled"}


class SmokeError(Exception):
    """冒烟失败。``code`` 即进程退出码。"""

    def __init__(self, msg: str, code: int = 1):
        super().__init__(msg)
        self.code = code


def _log(msg: str) -> None:
    print(f"[smoke {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _collect_urls(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value.startswith(("/", "http://", "https://")) else []
    if isinstance(value, dict):
        return [u for v in value.values() for u in _collect_urls(v)]
    if isinstance(value, list):
        return [u for v in value for u in _collect_urls(v)]
    return []


def login(client: httpx.Client, web: str, user_pass: str) -> str:
    user, _, password = user_pass.partition(":")
    r = client.post(f"{web}/auth/login", json={"username": user, "password": password})
    if r.status_code != 200:
        raise SmokeError(f"登录失败 {r.status_code}: {r.text[:200]}", 2)
    token = (r.json().get("data") or {}).get("token")
    if not token:
        raise SmokeError(f"登录响应里没有 token: {r.text[:200]}", 2)
    return token


def run(
    web: str,
    *,
    token: str = "",
    user_pass: str = "",
    storage: str = "",
    task_type: str = "demo",
    params: dict | None = None,
    timeout: float = 120.0,
    keep: bool = False,
    poll: float = 1.0,
) -> dict:
    """执行一次冒烟，返回最终任务详情；失败抛 ``SmokeError``。"""
    web = web.rstrip("/")
    storage = storage.rstrip("/")
    params = {"seconds": 1} if params is None else params
    with httpx.Client(timeout=15) as c:
        try:
            h = c.get(f"{web}/health")
        except httpx.HTTPError as e:
            raise SmokeError(f"Web 不可达 {web}: {e}", 2) from None
        if h.status_code != 200:
            raise SmokeError(f"{web}/health 返回 {h.status_code}", 2)
        _log(f"Web 可达 {web}")

        if not token:
            if not user_pass:
                raise SmokeError("需要 --token 或 --login 用户名:密码", 2)
            token = login(c, web, user_pass)
            _log("登录成功")
        headers = {"Authorization": f"Bearer {token}"}

        r = c.post(f"{web}/api/tasks", headers=headers, json={
            "task_type": task_type, "name": "smoke", "submit": True, "params_text": json.dumps(params),
        })
        if r.status_code != 201:
            raise SmokeError(f"创建任务失败 {r.status_code}: {r.text[:300]}", 2)
        tid = r.json()["data"]["id"]
        _log(f"已创建并提交任务 {tid}（type={task_type}）")

        last, deadline, task = None, time.time() + timeout, {}
        try:
            while time.time() < deadline:
                g = c.get(f"{web}/api/tasks/{tid}", headers=headers)
                if g.status_code != 200:
                    raise SmokeError(f"查询任务失败 {g.status_code}: {g.text[:200]}")
                task = g.json()["data"]
                state = (task["status"], task.get("sub_status") or "")
                if state != last:
                    _log(f"状态 → {state[0]}" + (f" / {state[1]}" if state[1] else ""))
                    last = state
                if task["status"] in TERMINAL:
                    break
                time.sleep(poll)
            else:
                hint = ("一直停在 pending：worker 没领到——查领取服务 /task/leases 的反代、worker 的 TASK_SERVER_URL 与能力声明"
                        if last and last[0] == "pending" else "")
                raise SmokeError(f"{timeout:.0f}s 内未到终态，最后状态 {last}。{hint}")

            if task["status"] != "completed":
                raise SmokeError(f"任务以 {task['status']} 结束: {task.get('error_message') or '(无错误信息)'}")

            result = task.get("result_path")
            result = json.loads(result) if isinstance(result, str) and result else (result or {})
            urls = _collect_urls(result)
            if not urls:
                _log("任务完成（result_path 中没有产物地址，跳过下载核对）")
            for u in urls:
                full = u if u.startswith("http") else f"{storage or web}{u}"
                d = c.get(full)
                if d.status_code != 200:
                    raise SmokeError(f"产物不可下载 {d.status_code}: {full}（相对地址需要 --storage 指向存储服务）")
                _log(f"产物可下载 {u} ({len(d.content)} 字节)")
            _log("✅ 全链路通：提交 → 领取 → 执行 → 回调 → 完成")
            return task
        finally:
            if not keep:
                try:
                    c.delete(f"{web}/api/tasks/{tid}", headers=headers)
                except httpx.HTTPError:
                    pass


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="webx 上线冒烟（会创建一个真实任务）")
    p.add_argument("--web", required=True, help="Web 服务地址，如 http://127.0.0.1:9009")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--token", default="", help="用户令牌")
    g.add_argument("--login", default="", metavar="用户名:密码", help="走 /auth/login 取令牌")
    p.add_argument("--storage", default="", help="存储服务地址；产物是相对地址时用它补全（默认回落到 --web）")
    p.add_argument("--task-type", default="demo", help="任务类型（默认 demo，worker 示例里的无业务任务）")
    p.add_argument("--params", default='{"seconds": 1}', help="任务参数 JSON")
    p.add_argument("--timeout", type=float, default=120.0, help="等待任务到终态的秒数")
    p.add_argument("--keep", action="store_true", help="结束后保留这个任务（默认删除）")
    a = p.parse_args(argv)
    try:
        params = json.loads(a.params)
    except json.JSONDecodeError as e:
        print(f"--params 不是合法 JSON: {e}", file=sys.stderr)
        return 2
    try:
        run(a.web, token=a.token, user_pass=a.login, storage=a.storage, task_type=a.task_type,
            params=params, timeout=a.timeout, keep=a.keep)
    except SmokeError as e:
        _log(f"❌ {e}")
        return e.code
    return 0


if __name__ == "__main__":
    sys.exit(main())
