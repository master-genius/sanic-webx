"""对一个**正在运行的**存储服务做端到端验证。

    python -m webx_storage verify --url http://127.0.0.1:9010 --token <STORAGE_INTERNAL_TOKEN>

和单元测试的区别：单元测试在进程内用测试客户端调用；这里走真实 HTTP，能验出
部署层面的问题——反代没转发某些路径、令牌配错、存储目录没写权限、Range 被网关吞掉……

所有测试数据都写在 ``__verify__/<随机串>/`` 下（任务产物则用 ``verify-<随机串>`` 任务号），
无论成败都会清理。退出码：0 全部通过，1 有失败，2 服务不可达 / 参数错误。
"""

from __future__ import annotations

import argparse
import os
import secrets
import sys
from typing import Callable

import httpx


class _Run:
    def __init__(self, base: str, token: str, timeout: float):
        self.base = base.rstrip("/")
        self.http = httpx.Client(timeout=timeout)
        self.auth = {"X-Internal-Token": token}
        self.failed: list[str] = []
        self.passed = 0

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        if ok:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failed.append(name)
            print(f"  FAIL  {name}  {detail}")
        return ok

    def req(self, method: str, path: str, *, auth: bool = True, **kw) -> httpx.Response:
        headers = {**(self.auth if auth else {}), **kw.pop("headers", {})}
        return self.http.request(method, self.base + path, headers=headers, **kw)


def _step_health(r: _Run) -> None:
    print("[1] 服务与鉴权")
    resp = r.req("GET", "/health", auth=False)
    r.check("GET /health 返回 200", resp.status_code == 200, f"got {resp.status_code}")
    resp = r.req("GET", "/internal/objects/__verify__/none", auth=False)
    r.check("缺令牌的内部请求被拒绝 (401)", resp.status_code == 401, f"got {resp.status_code}")
    resp = r.req("GET", "/internal/objects/__verify__/none", headers={"X-Internal-Token": "definitely-wrong"})
    r.check("错误令牌被拒绝 (401)", resp.status_code == 401, f"got {resp.status_code}")


def _step_objects(r: _Run, ns: str) -> None:
    print("[2] 通用对象")
    key, payload = f"{ns}/obj.bin", bytes(range(200))
    resp = r.req("POST", "/internal/objects", data={"key": key}, files={"file": ("obj.bin", payload)})
    r.check("创建对象 (201)", resp.status_code == 201 and resp.json().get("key") == key, resp.text[:120])
    resp = r.req("GET", f"/internal/objects/{key}")
    r.check("读取对象，内容逐字节一致", resp.status_code == 200 and resp.content == payload)
    resp = r.req("GET", f"/internal/objects/{key}", headers={"Range": "bytes=10-19"})
    r.check("Range 返回 206 且区间正确", resp.status_code == 206 and resp.content == payload[10:20],
            f"got {resp.status_code}（网关可能吞了 Range）")
    resp = r.req("HEAD", f"/internal/objects/{key}")
    r.check("HEAD 返回长度", resp.status_code == 200 and resp.headers.get("content-length") == "200")
    r.check("越界键被拒绝 (400)",
            r.req("POST", "/internal/objects", data={"key": "../x"}, files={"file": ("x", b"1")}).status_code == 400)
    r.check("删除对象", r.req("DELETE", f"/internal/objects/{key}").json().get("deleted") is True)
    resp = r.req("DELETE", f"/internal/objects/{key}")
    r.check("删除幂等（再删仍 200）", resp.status_code == 200 and resp.json().get("deleted") is False)


def _step_uploads(r: _Run, ns: str) -> None:
    print("[3] 分片上传与断点续传")
    key = f"{ns}/chunked.bin"
    resp = r.req("POST", "/internal/uploads", json={"key": key, "total": 8})
    if not r.check("创建会话 (201)", resp.status_code == 201, resp.text[:120]):
        return
    uid = resp.json()["upload_id"]
    resp = r.req("PATCH", f"/internal/uploads/{uid}", content=b"abcd", headers={"Upload-Offset": "0"})
    r.check("追加分片，Upload-Offset 头 = 4", resp.headers.get("upload-offset") == "4", str(dict(resp.headers)))
    resp = r.req("PATCH", f"/internal/uploads/{uid}", content=b"zz", headers={"Upload-Offset": "99"})
    r.check("offset 冲突 → 409，且头里带真实 offset",
            resp.status_code == 409 and resp.headers.get("upload-offset") == "4", f"got {resp.status_code}")
    resp = r.req("GET", f"/internal/uploads/{uid}")
    r.check("断线重连：查进度得到 4", resp.json().get("offset") == 4)
    r.req("PATCH", f"/internal/uploads/{uid}", content=b"efgh", headers={"Upload-Offset": "4"})
    resp = r.req("POST", f"/internal/uploads/{uid}/complete")
    r.check("完成上传", resp.status_code == 200 and resp.json().get("size") == 8, resp.text[:120])
    r.check("落位后内容完整", r.req("GET", f"/internal/objects/{key}").content == b"abcdefgh")
    r.req("DELETE", f"/internal/objects/{key}")
    uid2 = r.req("POST", "/internal/uploads", json={"key": f"{ns}/abort.bin"}).json().get("upload_id", "")
    r.check("放弃会话，幂等",
            r.req("DELETE", f"/internal/uploads/{uid2}").status_code == 200
            and r.req("DELETE", f"/internal/uploads/{uid2}").status_code == 200)


def _step_signature(r: _Run, ns: str) -> None:
    print("[4] 签名下载")
    key = f"{ns}/signed.txt"
    r.req("POST", "/internal/objects", data={"key": key}, files={"file": ("s.txt", b"signed-content")})
    resp = r.req("POST", "/internal/signatures", json={"key": key, "ttl_seconds": 60})
    if not r.check("签发签名", resp.status_code == 200 and "sign" in resp.json(), resp.text[:120]):
        return
    url = resp.json()["url"]
    resp = r.req("GET", url, auth=False)
    r.check("无需令牌，凭签名下载", resp.status_code == 200 and resp.content == b"signed-content", f"got {resp.status_code}")
    r.check("无签名 → 404", r.req("GET", f"/public/objects/{key}", auth=False).status_code == 404)
    r.check("伪造签名 → 404", r.req("GET", f"/public/objects/{key}?sign=forged", auth=False).status_code == 404)
    r.req("DELETE", f"/internal/objects/{key}")


def _step_task_results(r: _Run, task: str) -> None:
    print("[5] 任务产物策略（task_results）")
    probe = r.req("POST", f"/internal/tasks/{task}/signatures", json={})
    if probe.status_code == 404:
        print("  SKIP  服务未启用 task_results 策略，跳过本节")
        return
    resp = r.req("POST", f"/internal/tasks/{task}/results", files={"file": ("out.zip", b"result-bytes")})
    if not r.check("创建结果文件 (201)", resp.status_code == 201, resp.text[:120]):
        return
    url = resp.json()["download_url"]
    r.check("download_url 指向公开路径", url == f"/public/tasks/{task}/results/out.zip", url)
    resp = r.req("GET", url, auth=False)
    r.check("公开下载", resp.status_code == 200 and resp.content == b"result-bytes")
    r.check("列表包含该文件",
            r.req("GET", f"/public/tasks/{task}/results", auth=False).json().get("files") == ["out.zip"])
    resp = r.req("POST", f"/internal/tasks/{task}/results/deletions", json={"urls": [url, "/public/objects/x"]})
    r.check("批量删除：1 删 1 跳过",
            resp.status_code == 200 and resp.json().get("deleted") == 1 and resp.json().get("skipped") == 1, resp.text[:160])
    r.check("删除后 404", r.req("GET", url, auth=False).status_code == 404)
    r.req("DELETE", f"/internal/tasks/{task}")


def run(base: str, token: str, timeout: float = 15.0) -> int:
    r = _Run(base, token, timeout)
    ns, task = f"__verify__/{secrets.token_hex(6)}", f"verify-{secrets.token_hex(6)}"
    steps: list[Callable[[], None]] = [
        lambda: _step_health(r),
        lambda: _step_objects(r, ns),
        lambda: _step_uploads(r, ns),
        lambda: _step_signature(r, ns),
        lambda: _step_task_results(r, task),
    ]
    try:
        for step in steps:
            step()
    except httpx.TransportError as e:
        print(f"\n无法连接 {base}: {e}")
        return 2
    finally:
        try:  # 无论成败都清理，不在被测服务里留垃圾
            r.req("DELETE", f"/internal/tasks/{task}")
        except httpx.HTTPError:
            pass

    print(f"\n通过 {r.passed} 项，失败 {len(r.failed)} 项")
    for name in r.failed:
        print(f"  ✗ {name}")
    return 1 if r.failed else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m webx_storage verify", description="对运行中的存储服务做端到端验证")
    p.add_argument("--url", default=os.environ.get("STORAGE_URL", "http://127.0.0.1:9010"))
    p.add_argument("--token", default=os.environ.get("STORAGE_INTERNAL_TOKEN", ""))
    p.add_argument("--timeout", type=float, default=15.0)
    args = p.parse_args(argv)
    if not args.token:
        print("缺少令牌：--token 或环境变量 STORAGE_INTERNAL_TOKEN")
        return 2
    return run(args.url, args.token, args.timeout)


if __name__ == "__main__":
    sys.exit(main())
