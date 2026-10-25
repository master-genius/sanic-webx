"""集成测试：真实子进程 → 真实 executor drain → 真实 ConcurrentLogWriter → 前端可见内容。

单测（test_log_redact_paths.py）只验 redact_for_frontend 这个纯函数；这里验的是**接线**：
  • app.build_executor 那样构造的 roots，能不能真的传到取内容的地方；
  • write() 记的任务目录，是不是执行期真实用到的那个目录；
  • 本地全量日志【不脱敏】、前端可见内容【脱敏】这条边界有没有被改坏。

子进程打印的是本机**真实**路径（当前 worker 包目录、真实家目录、真实任务目录），
断言它们一个字都不出现在前端可见内容里——比构造假路径更接近真实泄露场景。
"""

import sys
from pathlib import Path

import webx_worker
from webx_worker.executor import GenericTaskExecutor
from webx_worker.log_writer import ConcurrentLogWriter, build_redact_roots

PKG_DIR = Path(webx_worker.__file__).resolve().parent
HOME = Path.home()
CONDA_ROOT = "/opt/conda-fake-root"
CHANNELS = {"job": {"env": "conda", "env_name": "sci_test_env", "script": "scripts/job.py"}}

# 子进程：打印一段以假乱真的失败日志，含 worker 安装路径 / conda 环境 / 家目录 /
# 任务目录 / 第三方安装路径 / 以及绝不能被改写的程序正文与 SMILES。
_CHILD = r'''
import sys
W = sys.argv[1]; H = sys.argv[2]; T = sys.argv[3]
out = sys.stdout.write
out("Reading file topol.tpr, VERSION 2021.5\n")
out("Potential Energy = -1.23456e+05 kJ/mol, performance 12.3 ns/day\n")
out("RDKit SMILES: F/C=C/F\n")
out("Traceback (most recent call last):\n")
out('  File "%s/executor.py", line 837, in main\n' % W)
out('  File "%s/envs/sci_test_env/lib/python3.11/site-packages/pkg/mod.py", line 1060\n' % "/opt/conda-fake-root")
out("cwd=%s/out\n" % T)
out("cache at %s/.cache/tool\n" % H)
out("Source file: /var/lib/engine-2021.5/src/mdrun/runner.cpp\n")
out("结果文件上传成功 download_url=/public/tasks/abc/results/x.dat\n")
sys.stdout.flush()
'''


class _StubCallback:
    internal_token = "t"
    base_url = "http://localhost"
    storage_url = "http://localhost"
    storage_internal_url = "http://localhost"


def _writer(work_dir: Path) -> ConcurrentLogWriter:
    """与 app.build_executor 同口径地构造（这正是本测试要验的接线）。"""
    return ConcurrentLogWriter(
        redact_roots=build_redact_roots(
            work_dir=str(work_dir), conda_root=CONDA_ROOT, channels=CHANNELS,
            worker_root=PKG_DIR, home=HOME,
        )
    )


async def test_redaction_end_to_end(tmp_path):
    task_id, task_dir = "t_n", tmp_path / "t_n"
    task_dir.mkdir(parents=True)
    writer = _writer(tmp_path)
    ex = GenericTaskExecutor(
        callback=_StubCallback(), log_writer=writer, heartbeat=None, http_client=None,
        work_base_dir=str(tmp_path), channels_config=CHANNELS, conda_root=CONDA_ROOT,
    )
    rc = await ex.run_subprocess(
        [sys.executable, "-c", _CHILD, str(PKG_DIR), str(HOME), str(task_dir)],
        task_id, task_dir, log_filename="run.log", timeout=60,
    )
    assert rc == 0, rc
    seen = writer.get_truncated_content(task_id) or ""

    # —— 1. 本机布局一个字都不许出现 ——
    for leak in (str(PKG_DIR), str(HOME), str(task_dir), CONDA_ROOT, "/var/lib/engine-2021.5"):
        assert leak not in seen, (leak, seen)

    # —— 2. 该有的标签都在，诊断价值不丢 ——
    assert "[WORKER]/executor.py" in seen, seen
    assert "[ENV]/lib/python3.11/site-packages/pkg/mod.py" in seen, seen
    assert "cwd=[TASK_DIR]/out" in seen, seen
    assert "[HOME]/.cache/tool" in seen, seen
    assert "…/mdrun/runner.cpp" in seen, seen

    # —— 3. 正文一个字都不许被改（误伤是这道脱敏唯一的真实风险）——
    assert "Potential Energy = -1.23456e+05 kJ/mol, performance 12.3 ns/day" in seen
    assert "Reading file topol.tpr, VERSION 2021.5" in seen
    assert "RDKit SMILES: F/C=C/F" in seen

    # —— 4. 基础设施行整行剔除 ——
    assert "download_url" not in seen and "结果文件上传成功" not in seen

    # —— 5. 本机全量日志【不脱敏】：排障要看真实路径 ——
    local = (task_dir / "run.log").read_text(encoding="utf-8")
    assert str(PKG_DIR) in local
    assert "结果文件上传成功" in local
    await writer.close_task(task_id)


async def test_task_dir_forgotten_after_close(tmp_path):
    """close_task 必须清 _task_dirs，否则长跑 worker 的字典只增不减（内存泄漏）。"""
    writer = _writer(tmp_path)
    task_dir = tmp_path / "t_x"
    task_dir.mkdir()
    await writer.write("t_x", str(task_dir / "run.log"), "hello\n")
    assert "t_x" in writer._task_dirs
    await writer.close_task("t_x")
    assert "t_x" not in writer._task_dirs
