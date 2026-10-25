"""对一个**正在运行的**领取服务做不改数据的验证。

    python -m task_server verify --url http://127.0.0.1:9020 --token <INTERNAL_TOKEN>

只做不会改动任何任务的检查——**绝不发起真实认领**（否则在线上会把真实任务领走）：

  1. ``GET /health`` 存活
  2. 缺令牌 → 401；错误令牌 → 401
  3. 带有效令牌但 slots 非法 → 400（说明可达且鉴权通过；令牌错则是 401）
  4. 缺 worker_id → 400
  5. ``POST /task/pokes`` 可达，且 ``/health`` 的计数 +1
  6. 数据库连通：以一个**不会匹配任何任务**的类型发起一次短领取，应当正常返回空列表而不是 503
     （类型名是随机串，没有任务会是这个类型；会挂满 hold 时间，故用 ``--skip-db-check`` 可跳过）

退出码：0 通过 / 1 有失败 / 2 服务不可达或参数错误。
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
import time

import httpx


def run(base: str, token: str, *, check_db: bool = True, timeout: float = 40.0) -> int:
    base = base.rstrip("/")
    passed, failed = 0, []

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal passed
        if ok:
            passed += 1
            print(f"  PASS  {name}")
        else:
            failed.append(name)
            print(f"  FAIL  {name}  {detail}")

    auth = {"X-Internal-Token": token, "X-Worker-Id": "verify-probe"}
    try:
        with httpx.Client(timeout=timeout) as c:
            print("[1] 服务与鉴权")
            r = c.get(base + "/health")
            check("GET /health 返回 200", r.status_code == 200 and r.json().get("service") == "task_server", r.text[:100])
            pokes_before = r.json().get("pokes", 0) if r.status_code == 200 else 0
            lease = base + "/task/leases"
            r = c.post(lease, json={"slots": [["x"]]}, headers={"X-Worker-Id": "p"})
            check("缺令牌 → 401", r.status_code == 401, f"got {r.status_code}")
            r = c.post(lease, json={"slots": [["x"]]}, headers={"X-Internal-Token": "wrong", "X-Worker-Id": "p"})
            check("错误令牌 → 401", r.status_code == 401, f"got {r.status_code}")

            print("[2] 请求校验（令牌有效才会走到这里）")
            r = c.post(lease, json={"slots": []}, headers=auth)
            check("非法 slots → 400（可达且令牌有效）", r.status_code == 400, f"got {r.status_code}（401=令牌与服务端不一致）")
            r = c.post(lease, json={"slots": [["x"]]}, headers={"X-Internal-Token": token})
            check("缺 worker_id → 400", r.status_code == 400, f"got {r.status_code}")

            print("[3] 唤醒通道")
            r = c.post(base + "/task/pokes", json={})
            check("POST /task/pokes 返回 200", r.status_code == 200, f"got {r.status_code}")
            after = c.get(base + "/health").json().get("pokes", 0)
            check("poke 计数 +1", after == pokes_before + 1, f"{pokes_before} → {after}")

            if check_db:
                print("[4] 数据库连通（随机类型，不会认领任何真实任务；会挂满 hold 时间）")
                probe = "verify-" + secrets.token_hex(6)
                t0 = time.monotonic()
                r = c.post(lease, json={"slots": [[probe]]}, headers=auth)
                ok = r.status_code == 200 and r.json() == {"tasks": [], "count": 0}
                check("挂起后返回空列表 200（503 = 数据库不可达）", ok, f"got {r.status_code} {r.text[:80]}")
                print(f"        （挂起 {time.monotonic() - t0:.1f}s）")
    except httpx.TransportError as e:
        print(f"\n无法连接 {base}: {e}")
        return 2

    print(f"\n通过 {passed} 项，失败 {len(failed)} 项")
    for name in failed:
        print(f"  ✗ {name}")
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m task_server verify", description="验证运行中的领取服务（不改数据）")
    p.add_argument("--url", default=os.environ.get("TASK_SERVER_URL", "http://127.0.0.1:9020"))
    p.add_argument("--token", default=os.environ.get("INTERNAL_TOKEN", ""))
    p.add_argument("--skip-db-check", action="store_true", help="跳过会挂满 hold 时间的数据库连通检查")
    p.add_argument("--timeout", type=float, default=40.0)
    args = p.parse_args(argv)
    if not args.token:
        print("缺少令牌：--token 或环境变量 INTERNAL_TOKEN")
        return 2
    return run(args.url, args.token, check_db=not args.skip_db_check, timeout=args.timeout)


if __name__ == "__main__":
    sys.exit(main())
