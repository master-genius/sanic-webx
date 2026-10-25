"""前端可见日志的**文件系统路径脱敏**：已知根精确替换 + 未命中路径折叠。

覆盖三件事：
  1. 已知根（任务目录 / 工作根 / worker 包目录 / conda 环境 / conda 根 / 家目录 / 脚本根）
     被替换成语义标签，且**长前缀优先**（任务目录不会被 [WORK_DIR] 先吃掉）。
  2. 未登记的绝对路径（外部程序的第三方安装路径）折叠成 `…/末尾两段`。
  3. **不误伤**——单位 `kJ/mol/nm`、日期 `2026/07/29`、分数 `1/2`、已替换出的
     `[WORKER]/tasks/...` 标签路径，一律不得被折叠或改写。第 3 条是这道脱敏唯一的
     真实风险点：兜底正则一旦贪心，会把 程序正文输出改花，比漏脱敏更难发现。
"""

import tempfile
from pathlib import Path


from webx_worker.log_writer import (  # noqa: E402
    _FS_PATH_ROOTS,
    ConcurrentLogWriter,
    build_redact_roots,
    redact_for_frontend,
)

CONDA_ROOT = "/home/bob/.miniconda3"
WORK_DIR = "/media/bob/ssd1/webx-worker-tasks"
CHANNELS = {
    "heavy": {"env": "conda", "env_name": "sci_env", "script": "scripts/heavy.py"},
    "light": {"env": "conda", "env_name": "sci_env", "script": "scripts/job.py"},
}

ROOTS = build_redact_roots(
    work_dir=WORK_DIR,
    conda_root=CONDA_ROOT,
    channels=CHANNELS,
    worker_root=Path("/media/bob/ssd1/alice/proj/worker"),
    home=Path("/home/bob"),
)


def r(text: str) -> str:
    return redact_for_frontend(text, ROOTS)


# ==================== 1. 已知根精确替换 ====================

def test_worker_root_replaced():
    line = '  File "/media/bob/ssd1/alice/proj/worker/scripts/job.py", line 814, in main'
    out = r(line)
    assert out == '  File "[WORKER]/scripts/job.py", line 814, in main'
    assert "alice" not in out and "bob" not in out


def test_conda_env_replaced_before_conda_root():
    """{conda_root}/envs/{name} 必须先于 {conda_root} 命中，否则环境名会漏出来。"""
    line = (
        "/home/bob/.miniconda3/envs/sci_env/lib/python3.10/"
        "site-packages/automd/ff/gaff.py"
    )
    out = r(line)
    assert out == "[ENV]/lib/python3.10/site-packages/automd/ff/gaff.py"
    assert "sci_env" not in out


def test_conda_root_replaced():
    out = r("using /home/bob/.miniconda3/bin/conda")
    assert out == "using [CONDA]/bin/conda"


def test_home_replaced():
    out = r("cache at /home/bob/.cache/xyz")
    assert out == "cache at [HOME]/.cache/xyz"


def test_work_dir_replaced():
    out = r(f"work base {WORK_DIR}/106t91d158x2ue0d/in")
    assert out == "work base [WORK_DIR]/106t91d158x2ue0d/in"


# ==================== 2. 未命中的绝对路径折叠 ====================

def test_listed_root_folded():
    """名单内的根下的未知路径 → 折叠成末尾两段，仍可定位。"""
    out = r("Source file: /media/disk1/alice/proj/src/engine/runner.cpp (line 123)")
    assert "…/engine/runner.cpp" in out
    assert "alice" not in out


def test_every_listed_root_folded():
    """遍历 _FS_PATH_ROOTS 本身——以后往名单加根目录，这条自动覆盖，不会漏测。"""
    for root in _FS_PATH_ROOTS:
        out = r(f"/{root}/aaa/bbb/ccc.py")
        # /home/<name> 会先被 _GENERIC_HOME_RE 收敛成 [HOME]（更强的规则，先命中）
        want = "[HOME]/bbb/ccc.py" if root == "home" else "…/bbb/ccc.py"
        assert out == want, (root, out)


def test_unlisted_root_untouched():
    """【设计如此】只认 _FS_PATH_ROOTS 里的根开头，其余绝对路径原样保留。

    兜底层要防的是**本机布局与身份泄露**，这类路径集中在名单里那几个根下。
    把 `/srv`、`/tmp` 之类也卷进来 = 用更大的误伤面换很低的收益。
    """
    for s in (
        "data in /srv/shared/project/file.dat",
        "tmpdir /tmp/build/abc/def",
        "mnt at /mnt/nas/share/data.bin",
    ):
        assert r(s) == s, s


def test_short_path_not_folded():
    """不足 3 段的路径不折（根 + 至少 2 段），避免把 /var/log 之类改花。"""
    assert r("see /tmp/x") == "see /tmp/x"
    assert r("in /var/log") == "in /var/log"


def test_root_name_must_be_whole_segment():
    """根名后必须紧跟 `/`：`/homework`、`/optional`、`/usrdata` 等不得被前缀误命中。

    名单里的根多是常见单词前缀（home/opt/usr/var），这条约束是它们能安全共存的前提；
    遍历常量本身，往名单加根时自动覆盖。
    """
    for root in _FS_PATH_ROOTS:
        s = f"/{root}xyz/a/b/c"
        assert r(s) == s, s


def test_non_ascii_path_folded():
    """路径段字符集必须 Unicode 感知：中文目录名否则一个字都遮不住，
    而这类路径恰恰最可能带用户真名。"""
    out = r("/media/工具/程序/bin/run.py")
    assert out == "…/bin/run.py"
    assert "工具" not in out


def test_non_ascii_home_masked():
    """中文用户名同样要被 [HOME] 收敛，否则折叠保留两段时它就落在倒数第二段。"""
    out = r("/home/张三/data/file.txt")
    assert out == "[HOME]/data/file.txt"
    assert "张三" not in out


def test_colon_in_path_folded():
    """冒号是合法路径字符，不能因为它在段内就整条漏掉。"""
    assert r("/var/foo:bar/pkg/mod.py") == "…/pkg/mod.py"


def test_quoted_path_still_folded_by_bare_rule():
    """引号不妨碍折叠：`"` 不在 lookbehind 的排除集里，traceback 的 File "..." 照常命中。
    （无需为引号单独加规则——加了反而会把 `"/api/tasks/<id>/files"` 这类路由模式改花。）"""
    out = r('File "/media/disk1/proj/src/engine/runner.cpp", line 12')
    assert out == 'File "…/engine/runner.cpp", line 12'


def test_bare_path_with_space_is_known_limit():
    """【已评估后不做·勿再加】含空格的裸路径不折叠。

    两条路都不划算：
      ① 把空格放进段内字符集 → `see /tmp/x and /var/y` 被当成一条 4 段路径、把 `and`
         折进去，改花正文；
      ② 对「双引号内以 / 开头的整串」单独处理 → 误伤面扩到所有引号包裹的路径形字符串
         （日志里打印的路由模式 `"/api/tasks/<id>/files"` 会被折成 `"…/<id>/files"`）。
    专门部署的服务里带空格的安装路径本就极罕见，用一片新的误伤面换近乎为零的收益不值。
    本用例把这条取舍连同它换来的东西一起固化。
    """
    s = "/media/My Disk/proj/tool/run.py"
    assert r(s) == s                                              # 代价：这条漏遮
    # 注：`/home/<带空格的用户名>` 会被 _GENERIC_HOME_RE 遮住第一段（`/home/My Name`
    # → `[HOME] Name`），属部分生效，不在本用例的取舍范围内。
    assert r("/home/My Name/x") == "[HOME] Name/x"
    assert r("see /tmp/x and /var/y") == "see /tmp/x and /var/y"  # 换来：散文不被改
    assert r('route "/api/tasks/<id>/files" registered') == \
        'route "/api/tasks/<id>/files" registered'                # 换来：路由模式不被改


# ==================== 3. 不误伤（关键） ====================

def test_units_not_touched():
    """程序正文里的单位/分数/日期不得被当成路径。"""
    for s in (
        "Potential Energy = -1.23456e+05 kJ/mol",
        "Pres. DC (bar) Pressure (bar) dVremain/dl",
        "Constr. rmsd 1/2 step, 3/4 done",
        "performance: 12.345 ns/day, 1.944 hour/ns",
        "started on 2026/07/29 at 10:22:44",
    ):
        assert r(s) == s, s


def test_replaced_label_not_folded_again():
    """①替换出的 `[WORKER]/scripts/job.py` 前有 `]`，不得被②再折叠成 `…/job.py`。"""
    out = r('File "/media/bob/ssd1/alice/proj/worker/scripts/job.py", line 1')
    assert "[WORKER]/scripts/job.py" in out
    assert "…" not in out


def test_no_roots_still_folds():
    """未登记根时降级：仍折叠绝对路径，不外泄本机布局。"""
    out = redact_for_frontend("at /media/bob/ssd1/alice/proj/worker/scripts/job.py")
    assert out == "at …/scripts/job.py"


def test_prefix_needs_path_boundary():
    """前缀替换必须按【路径段】而非子串：/home/ps 不得把 /home/bob2/x 改成 [HOME]dz2/x。"""
    roots = build_redact_roots(
        work_dir="", conda_root="", channels={},
        worker_root=Path("/nonexistent/w"), home=Path("/home/ps"),
    )
    out = redact_for_frontend("/home/ps/a/b and /home/bob2/x/y/z", roots)
    assert "[HOME]dz2" not in out          # 不得截断到路径段中间
    assert out.startswith("[HOME]/a/b")    # 自己的家目录仍精确替换
    assert "bob2" not in out              # 别人的家目录由通用形态兜住


def test_known_root_requires_unicode_path_boundary():
    """边界断言的字符集必须含 `\\w` 的 Unicode 部分。

    否则 `/home/user` 这个已知根会在 `/home/user张三/...` 上中途命中，产出
    `[HOME]张三/...`——名字照样漏出去，正文还被改花。收严后该行落到
    _GENERIC_HOME_RE 兜底，整段 `/home/user张三` 被收敛成 [HOME]。
    """
    out = redact_for_frontend("/home/user张三/private/file.txt", (("/home/user", "[HOME]"),))
    assert out == "[HOME]/private/file.txt"
    assert "张三" not in out


def test_colon_is_path_char_when_followed_by_path_char():
    """冒号后面还跟路径字符 → 视作路径的一部分，不做段中途替换，交兜底折叠。

    否则 `/opt/tool` 会在 `/opt/tool:beta/...` 上中途命中，产出 `[WORKER]:beta/...`。
    """
    out = redact_for_frontend("/opt/tool:beta/pkg/run.py", (("/opt/tool", "[WORKER]"),))
    assert out == "…/pkg/run.py"
    assert ":beta" not in out


def test_colon_after_known_root_is_a_known_tradeoff():
    """【已知取舍】冒号一律算路径字符，故 `<已知根>: <消息>` 不替换成语义标签。

    代价：`/media/.../worker: denied` 拿不到 `[WORKER]:`，退化成兜底折叠
    `…/proj/worker: denied`。**不泄露**——真实的根都够深且以名单内顶级目录开头，
    兜底必然接住；只是标签精细度下降。
    换来的是边界断言与段内字符集共用同一个常量、无嵌套前瞻。要恢复标签需按
    「冒号后是否还跟路径字符」二分并把常量拆两级，为一个诊断细节不值，故不做。
    """
    roots = (("/media/d1/proj/worker", "[WORKER]"),)
    out = redact_for_frontend("/media/d1/proj/worker: permission denied", roots)
    assert out == "…/proj/worker: permission denied"      # 兜底接住，无泄露
    assert "[WORKER]" not in out                          # 标签确实退化了


def test_generic_home_uses_same_segment_charset():
    """通用家目录的用户名段与 _SEG_CHARS 同源：窄于它就会在段中途命中。

    历史缺陷：用户名段写成 `[\\w.\\-]+` 时，`:` `+` `@` 被当成段边界 →
    `/home/user:x/data/y` 产出 `[HOME]:x/data/y`，正文被改花且只遮住半个名字。
    """
    for user in ("user:x", "user+name", "user@host", "用户张三"):
        out = r(f"/home/{user}/data/y")
        assert out == "[HOME]/data/y", (user, out)


def test_all_three_patterns_share_one_charset():
    """结构不变量：段内 / 通用家目录 / 边界断言三处**必须**同源 `_SEG_CHARS`。

    这是同一类缺陷的根因——三处各写各的字符集，差集里的字符就成了「段边界」，
    产生段中途替换。断言正则文本里确实嵌着同一个常量，防止有人图省事再抄一份字面量。

    """
    from webx_worker import log_writer as lw

    assert lw._SEG_CHARS in lw._FS_PATH_RE.pattern
    assert lw._SEG_CHARS in lw._GENERIC_HOME_RE.pattern
    assert lw._SEG_CHARS in lw._PATH_BOUNDARY


def test_other_users_home_masked():
    """②折叠保留末尾两段时用户名会落在倒数第二段，故 /home/<name> 须先被收敛。"""
    out = r("third-party at /home/someoneelse/tool_5_0/tool")
    assert "someoneelse" not in out
    assert out == "third-party at [HOME]/tool_5_0/tool"


def test_smiles_slashes_not_touched():
    """日志里会出现 SMILES，其顺反异构用 / 和 \\，不得被当成路径。"""
    for s in (
        "RDKit 无法解析 SMILES: 'F/C=C/F'",
        "canonical: C/C=C/C(=O)O",
    ):
        assert r(s) == s, s


# ==================== 4. 既有脱敏不回归 ====================

def test_existing_infra_line_still_dropped():
    text = "正常输出\n结果文件上传成功 download_url=/public/tasks/abc/results/x.dat\n收尾"
    assert r(text) == "正常输出\n收尾"


def test_existing_url_still_masked():
    assert "[redacted]" in r("fetch https://example.com/a/b/c done")


# ==================== 5. 任务目录：长前缀优先 ====================

def test_script_root_is_a_known_root():
    """任务脚本根目录（channel 配置所在目录）会出现在 traceback 里，必须被替换成语义标签。"""
    roots = build_redact_roots(
        work_dir="", conda_root="", channels={}, worker_root=Path("/nonexistent/w"),
        home=Path("/nonexistent/h"), script_root="/srv-scripts/deploy/alice/jobs",
    )
    out = redact_for_frontend('File "/srv-scripts/deploy/alice/jobs/job.py", line 3', roots)
    assert out == 'File "[SCRIPTS]/job.py", line 3'


def test_task_dir_wins_over_work_dir():
    """任务目录在 WORK_DIR 之下，必须先命中，否则看不出「这是任务目录」。"""
    with tempfile.TemporaryDirectory() as td:
        task_id = "106t91d158x2ue0d"
        task_dir = Path(td) / task_id
        task_dir.mkdir()
        w = ConcurrentLogWriter(
            redact_roots=build_redact_roots(
                work_dir=td, conda_root=CONDA_ROOT, channels=CHANNELS,
                worker_root=Path("/nonexistent/worker"), home=Path("/nonexistent/home"),
            )
        )
        import asyncio

        asyncio.run(w.write(task_id, str(task_dir / "run.log"), f"cwd={task_dir}/out\n"))
        out = w.get_truncated_content(task_id)
        assert out == "cwd=[TASK_DIR]/out"
        asyncio.run(w.close_task(task_id))

