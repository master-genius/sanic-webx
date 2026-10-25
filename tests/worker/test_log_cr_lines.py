"""LineAssembler / TruncatedLogWriter 的回车（\\r）语义测试。

覆盖点：
  1. \\r 是"回车覆盖当前行"而非换行 —— 一段进度刷新只产出一行
  2. \\r\\n 不产生空行（旧实现靠先合并 \\r\\n 规避，新实现靠 \\r 重置光标列）
  3. 活动行在截断缓冲里只占一个槽位，且 total_lines 不随刷新次数增长
  4. head/tail 边界与 EOF 收尾不产生重复行
"""

import sys


from webx_worker.log_writer import LineAssembler, TruncatedLogWriter  # noqa: E402


def _drain(text_chunks, writer=None):
    """把若干 chunk 喂给装配器 + 写入器，返回 (事件列表, 写入器)。

    chunk 可传 str（自动 utf-8 编码）或 bytes（用于验证多字节切断场景）。
    """
    asm = LineAssembler()
    writer = writer or TruncatedLogWriter(head_lines=100, tail_lines=2000)
    events = []
    for chunk in text_chunks:
        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8")
        for ln, partial in asm.feed(chunk):
            events.append((ln, partial))
            writer.write(ln, partial=partial)
    for ln, partial in asm.close():
        events.append((ln, partial))
        writer.write(ln, partial=partial)
    return events, writer


def _lines(writer):
    return writer.get_content().split("\n") if writer.get_content() else []


# ---------- 1. \r 覆盖语义 ----------

def test_cr_overwrites_instead_of_newline():
    _, w = _drain(["step 1\rstep 2\rstep 300\nWriting final coordinates.\n"])
    assert _lines(w) == ["step 300", "Writing final coordinates."]
    assert w.total_lines == 2


def test_cr_overwrite_keeps_longer_tail():
    """短内容覆盖长内容时，未被覆盖的尾部保留 —— 与终端渲染一致。"""
    _, w = _drain(["abcdefgh\rXY\n"])
    assert _lines(w) == ["XYcdefgh"]


# ---------- 2. \r\n 不产生空行 ----------

def test_crlf_yields_single_line_no_blank():
    _, w = _drain(["abc\r\ndef\r\n"])
    assert _lines(w) == ["abc", "def"]
    assert w.total_lines == 2


# ---------- 3. 跨 chunk 的活动行只占一个槽位 ----------

def test_progress_stream_collapses_to_one_line():
    """1000 次进度刷新 —— 旧实现产出 1000 行，新实现只占 1 行。"""
    chunks = [f"step {i}, remaining wall clock time: {1000 - i} s\r" for i in range(1000)]
    _, w = _drain(chunks)
    lines = _lines(w)
    assert len(lines) == 1
    assert lines[0].startswith("step 999,")
    assert w.total_lines == 1


def test_partial_then_final_has_no_duplicate():
    """活动行先被快照、后被 \\n 定版：定版覆盖同一槽位，不留重复行。"""
    _, w = _drain(["step 1\r", "step 2\r", "step 3\n", "done\n"])
    assert _lines(w) == ["step 3", "done"]
    assert w.total_lines == 2


def test_line_after_final_opens_new_slot():
    """定版后的下一条活动行必须开新槽位，不能覆盖已定版的行。"""
    _, w = _drain(["first\n", "prog 1\r", "prog 2\r"])
    assert _lines(w) == ["first", "prog 2"]
    assert w.total_lines == 2


# ---------- 4. 边界 ----------

def test_live_slot_survives_head_tail_boundary():
    """活动行恰好写满 head 时，后续刷新仍覆盖 head 里的原槽位。"""
    w = TruncatedLogWriter(head_lines=3, tail_lines=10)
    _drain([f"L{i}\n" for i in range(2)] + ["prog 1\r", "prog 2\r", "prog 3\n", "after\n"], w)
    assert _lines(w) == ["L0", "L1", "prog 3", "after"]
    assert w.total_lines == 4


def test_eof_finalizes_pending_line_once():
    """EOF 时残留活动行定版，不产生重复。"""
    _, w = _drain(["tail without newline"])
    assert _lines(w) == ["tail without newline"]
    assert w.total_lines == 1


def test_eof_after_partial_snapshot_no_duplicate():
    _, w = _drain(["prog 1\r", "prog 2"])
    assert _lines(w) == ["prog 2"]
    assert w.total_lines == 1


def test_line_split_across_chunks():
    """普通行被 chunk 边界切断时正常拼接。"""
    _, w = _drain(["hel", "lo wor", "ld\n"])
    assert _lines(w) == ["hello world"]


def test_oversized_line_is_force_finalized():
    """无 \\r 也无 \\n 的超长输出被强制定版，行缓冲不无界增长。"""
    asm = LineAssembler()
    events = asm.feed(b"x" * (64 * 1024 + 10))
    assert events and events[0][1] is False  # 已定版
    assert asm.line == ""


def test_reset_clears_column_too():
    """异常兜底的 reset 必须同时清光标列，否则后续行会被错位覆盖。"""
    asm = LineAssembler()
    asm.feed(b"abcdefgh")
    asm.reset()
    assert asm.feed(b"XY\n") == [("XY", False)]


# ---------- 5. 增量推送水位 ----------

def test_write_count_tracks_refreshes_for_streaming():
    """进度刷新时行数不变但修订号在涨 —— 否则 _stream_log 会长时间跳过推送、前端假死。"""
    asm = LineAssembler()
    w = TruncatedLogWriter()
    marks = []
    for i in range(50):
        for ln, partial in asm.feed(f"step {i}\r".encode("utf-8")):
            w.write(ln, partial=partial)
        marks.append(w.writes)

    assert w.total_lines == 1                      # 截断缓冲里只占一行
    assert marks == sorted(set(marks)) == marks    # 每次刷新水位都严格递增
    assert len(marks) == 50


# ---------- 6. chunk 边界不变量 ----------

_GMX_STREAM = (
    "starting mdrun 'system'\n"
    "step 0\rstep 100, remaining wall clock time:    12 s\r"
    "step 200, remaining wall clock time:     8 s\r"
    "step 300, remaining wall clock time:     0 s\r\n"
    "Writing final coordinates.\n"
    "               Core t (s)   Wall t (s)        (%)\n"
)


def test_result_invariant_across_chunk_boundaries():
    """8KiB 定长读会把 \\r / \\n 切在任意位置 —— 结果必须与一次性喂入完全一致。"""
    _, whole = _drain([_GMX_STREAM])
    expected = _lines(whole)
    assert expected == [
        "starting mdrun 'system'",
        "step 300, remaining wall clock time:     0 s",
        "Writing final coordinates.",
        "               Core t (s)   Wall t (s)        (%)",
    ]
    raw = _GMX_STREAM.encode("utf-8")
    for cut in range(1, len(raw)):
        _, w = _drain([raw[:cut], raw[cut:]])
        assert _lines(w) == expected, f"chunk 边界 {cut} 处结果不一致"
        assert w.total_lines == len(expected)


# ---------- 7. 多字节字符被 chunk 切断 ----------

_CN_STREAM = "分子 'M01' 缺少 residue_name——该值应由服务端物化。\n处理中\rG\n"


def test_multibyte_char_split_across_chunks():
    """定长 read(8192) 会把中文切成两半：增量解码器必须跨 chunk 拼回，不得出现 U+FFFD。

    子进程 stdout 确有中文（任务脚本抛出的中文异常回溯），逐 chunk 各自 decode 会乱码。
    """
    raw = _CN_STREAM.encode("utf-8")
    _, whole = _drain([raw])
    expected = _lines(whole)
    assert expected == ["分子 'M01' 缺少 residue_name——该值应由服务端物化。", "G理中"]

    for cut in range(1, len(raw)):
        _, w = _drain([raw[:cut], raw[cut:]])
        got = _lines(w)
        assert "�" not in "".join(got), f"字节边界 {cut} 处出现替换字符（乱码）"
        assert got == expected, f"字节边界 {cut} 处结果不一致: {got}"


def test_reset_keeps_decoder_state():
    """异常兜底的 reset 不能清解码器：它持有的是半个字符，清掉反而制造乱码。"""
    raw = "中".encode("utf-8")
    asm = LineAssembler()
    asm.feed(raw[:1])   # 半个字符留在解码器里
    asm.reset()         # 模拟写盘异常后的兜底
    assert asm.feed(raw[1:] + b"\n") == [("中", False)]


# ---------- 8. 退化配置与性能路径 ----------

def test_zero_tail_lines_does_not_crash():
    """LOG_TAIL_LINES 是 env 可配的；配成 0 时 deque 不保留元素，覆盖不得抛 IndexError。"""
    w = TruncatedLogWriter(head_lines=1, tail_lines=0)
    _drain([f"L{i}\n" for i in range(3)] + ["p1\r", "p2\r", "p3\n"], w)
    assert w.total_lines == 4  # 计数照常，不因缓冲退化而错乱


def test_full_line_overwrite_is_exact():
    """整行覆盖走的是赋值快路径 —— 结果必须与逐字符覆盖完全一致。"""
    # 等长覆盖
    _, w = _drain(["aaaa\rbbbb\n"])
    assert _lines(w) == ["bbbb"]
    # 更长覆盖
    _, w = _drain(["aa\rbbbb\n"])
    assert _lines(w) == ["bbbb"]
    # 更短覆盖（走慢路径，保留尾部）
    _, w = _drain(["aaaa\rbb\n"])
    assert _lines(w) == ["bbaa"]
    # 覆盖后继续追加（光标不在行首）
    _, w = _drain(["aaaaaa\rbb", "cc\n"])
    assert _lines(w) == ["bbccaa"]


def test_long_line_high_frequency_refresh_is_bounded():
    """长行 × 高频刷新：快路径避免 O(len) 反复拼接，且行长不随刷新次数增长。"""
    asm = LineAssembler()
    long_line = "x" * 4096
    for _ in range(500):
        asm.feed(long_line.encode("utf-8") + b"\r")
    assert len(asm.line) == 4096


# ---------- 9. 与省略计数的一致性 ----------

def test_omitted_count_unaffected_by_refreshes():
    """活动行刷新不计入 total_lines，故 "省略 N 行" 的算术不漂移。"""
    w = TruncatedLogWriter(head_lines=2, tail_lines=2)
    _drain([f"L{i}\n" for i in range(10)] + ["p1\r", "p2\r", "p3\n"], w)
    assert w.total_lines == 11  # 10 条普通行 + 1 条活动行
    content = w.get_content()
    assert "[省略 7 行]" in content  # 11 - 2 - 2
    assert content.split("\n")[-1] == "p3"


if __name__ == "__main__":
    import traceback

    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception:
                fails += 1
                print(f"FAIL {name}")
                traceback.print_exc()
    print(f"\n{'ALL PASS' if not fails else f'{fails} FAILED'}")
    sys.exit(1 if fails else 0)
