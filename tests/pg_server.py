"""测试用的 PostgreSQL：按需拉起一个用户态的临时实例。

认领 / 租约的 SQL 是 PG 专有的（``FOR UPDATE SKIP LOCKED``、``now()``、``EXTRACT(EPOCH ...)``），
sqlite 测不了——而它们恰恰是全系统互斥性的来源，必须真跑。

获取 PG 的三种方式，按优先级：

  1. ``WEBX_TEST_PG_URL=postgres://user:pass@host:port/dbname`` —— 用你已有的实例（库需为空，测试会建表并清空数据）
  2. 找到 PG 二进制 —— ``WEBX_PG_BIN`` 指向含 ``initdb`` / ``postgres`` 的目录，或 PATH 里有 ``pg_ctl``，
     或 ``~/.cache/webx-pg``（见 docs/testing.md 的"嵌入式 PG"一节）
  3. 都没有 → 测试**失败**并说明怎么办。**不会静默跳过**（跳过等于认领逻辑没被验证却显示绿色）；
     确需跳过请显式设 ``WEBX_SKIP_PG_TESTS=1``

以 root 运行时 PG 拒绝启动，所以用 ``setpriv`` 降权到一个普通用户（默认 ``pgserver``，可用 ``WEBX_PG_USER`` 指定）。
另一个现实问题：个别环境里 ``/dev/null`` 被误建成了普通文件，非 root 进程写它会 Permission denied，
``initdb`` 因此失败。检测到时在**私有挂载命名空间**里给 PG 提供一个正常的 null 设备——不改宿主机。
"""

from __future__ import annotations

import glob
import os
import shutil
import socket
import stat
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


class PgUnavailable(RuntimeError):
    """当前环境拿不到可用的 PostgreSQL。"""


@dataclass
class PgInstance:
    dsn: str
    stop: callable


def find_pg_bin() -> Path | None:
    env = os.environ.get("WEBX_PG_BIN")
    if env and (Path(env) / "initdb").exists():
        return Path(env)
    pattern = str(Path.home() / ".cache/webx-pg/pgenv/lib/python*/site-packages/pgserver/pginstall/bin")
    for hit in sorted(glob.glob(pattern)):
        if (Path(hit) / "initdb").exists():
            return Path(hit)
    which = shutil.which("pg_ctl")
    if which and (Path(which).parent / "initdb").exists():
        return Path(which).parent
    return None


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _dev_null_is_broken() -> bool:
    try:
        return not stat.S_ISCHR(os.stat("/dev/null").st_mode)
    except OSError:
        return True


# 在私有挂载命名空间里造一个正常的 null 设备再 exec 目标命令（不改宿主机的 /dev）
_NS_PRELUDE = (
    "set -e; d=$(mktemp -d); mount -t tmpfs tmpfs \"$d\"; mknod -m 666 \"$d/null\" c 1 3; "
    "mount --bind \"$d/null\" /dev/null; exec \"$@\""
)


def _wrap(cmd: list[str], run_as: str | None) -> list[str]:
    if run_as:
        cmd = ["setpriv", f"--reuid={run_as}", f"--regid={run_as}", "--init-groups", *cmd]
    if _dev_null_is_broken():
        if os.geteuid() != 0 or not shutil.which("unshare"):
            raise PgUnavailable(
                "/dev/null 不是字符设备，PG 无法初始化；需要 root + unshare 才能在私有命名空间里绕过，"
                "或先修复 /dev/null（mknod -m 666 /dev/null c 1 3）"
            )
        cmd = ["unshare", "--mount", "bash", "-c", _NS_PRELUDE, "bash", *cmd]
    return cmd


def start_pg(workdir: Path | None = None) -> PgInstance:
    """拉起一个临时 PG 实例，返回 DSN 与停止函数（停止时清理其数据目录）。``workdir`` 保留仅为兼容，不再使用。"""
    bindir = find_pg_bin()
    if bindir is None:
        raise PgUnavailable(
            "找不到 PostgreSQL。任选其一：\n"
            "  • 设 WEBX_TEST_PG_URL 指向一个空库；\n"
            "  • 设 WEBX_PG_BIN 指向含 initdb/postgres 的目录；\n"
            "  • 按 docs/testing.md 安装嵌入式 PG 到 ~/.cache/webx-pg。\n"
            "（确需跳过这些测试请显式设 WEBX_SKIP_PG_TESTS=1）"
        )

    run_as = None
    if os.geteuid() == 0:
        run_as = os.environ.get("WEBX_PG_USER", "pgserver")
        import pwd

        try:
            pwd.getpwnam(run_as)
        except KeyError:
            raise PgUnavailable(f"以 root 运行时 PG 需要降权到普通用户，但用户 {run_as!r} 不存在（WEBX_PG_USER 可指定）")

    # 自己建专用目录而不用 pytest 的 tmp_path：后者是 0700 属于当前用户，降权后的 PG 用户进不去
    import tempfile

    root = Path(tempfile.mkdtemp(prefix="webx-pg-"))
    os.chmod(root, 0o755)
    data, sock = root / "data", root / "sock"
    sock.mkdir()
    if run_as:
        import pwd

        pw = pwd.getpwnam(run_as)
        os.chown(root, pw.pw_uid, pw.pw_gid)
        os.chown(sock, pw.pw_uid, pw.pw_gid)

    def run(cmd: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(_wrap(cmd, run_as), capture_output=True, text=True, timeout=120)

    init = run([str(bindir / "initdb"), "-D", str(data), "--auth=trust", "-U", "postgres",
                "--encoding=utf8", "--locale=C"])
    if init.returncode != 0:
        raise PgUnavailable(f"initdb 失败:\n{init.stdout[-800:]}\n{init.stderr[-800:]}")

    port = _free_port()
    proc = subprocess.Popen(
        _wrap(
            [str(bindir / "postgres"), "-D", str(data), "-p", str(port), "-k", str(sock),
             "-c", "listen_addresses=127.0.0.1", "-c", "fsync=off", "-c", "synchronous_commit=off",
             "-c", "full_page_writes=off", "-c", "max_connections=100", "-c", "shared_buffers=64MB"],
            run_as,
        ),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True,
    )

    def stop() -> None:
        import signal

        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
        shutil.rmtree(root, ignore_errors=True)

    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read().decode(errors="replace")[-1500:]
            raise PgUnavailable(f"postgres 进程提前退出:\n{out}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                break
        except OSError:
            time.sleep(0.2)
    else:
        stop()
        raise PgUnavailable("postgres 30 秒内未开始监听")

    created = run([str(bindir / "createdb"), "-h", "127.0.0.1", "-p", str(port), "-U", "postgres", "webx_test"])
    if created.returncode != 0:
        stop()
        raise PgUnavailable(f"createdb 失败: {created.stderr[-500:]}")

    return PgInstance(dsn=f"postgres://postgres@127.0.0.1:{port}/webx_test", stop=stop)
