"""HTTP Range 解析 —— 支持分段下载 / 断点续传。

纯函数、零依赖。返回对象满足 Sanic ``file_stream(_range=...)`` 的协议
（``start`` / ``end`` / ``size`` / ``total``）。

只实现单段 ``bytes=``：多段 byteranges 极少被真实客户端用到，实现它要拼
multipart/byteranges 响应体，收益不抵复杂度。遇到多段或非 bytes 单位一律
**降级为全量 200**（鲁棒不报错），而不是回 416——那会让正常下载直接失败。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class ByteRange:
    """单段字节区间，结构上满足 Sanic file_stream 的 Range 协议。"""

    start: int
    end: int
    size: int
    total: int


# 三态返回：
#   None            → 按全量 200 处理（无 Range / 语法非法 / 多段）
#   "unsatisfiable" → 回 416，并按规范附 Content-Range: bytes */total
#   ByteRange       → 206 局部传输
RangeResult = Union[None, str, ByteRange]

UNSATISFIABLE = "unsatisfiable"


def parse_byte_range(range_header: Optional[str], file_size: int) -> RangeResult:
    """解析 HTTP ``Range`` 头（仅单段 ``bytes=``）。

    支持三种写法：
      - ``bytes=500-999``  指定区间
      - ``bytes=1000-``    从 1000 到末尾
      - ``bytes=-500``     末尾 500 字节

    Args:
        range_header: 原始 Range 头，可为 None。
        file_size: 文件总字节数。

    Returns:
        见 ``RangeResult`` 的三态说明。
    """
    if not range_header:
        return None

    range_header = range_header.strip()
    if not range_header.startswith("bytes="):
        return None

    spec = range_header[len("bytes="):].strip()
    # 多段 byteranges 不实现，降级全量
    if "," in spec or "-" not in spec:
        return None

    start_s, _, end_s = spec.partition("-")
    start_s, end_s = start_s.strip(), end_s.strip()

    try:
        if start_s == "":
            # 后缀区间 bytes=-N：最后 N 字节
            suffix = int(end_s)
            if suffix <= 0:
                return None
            start = max(0, file_size - suffix)
            end = file_size - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s else file_size - 1
    except ValueError:
        return None

    if start < 0:
        return None

    # 越界起点必须先判（含 bytes=N- 且 N >= 文件大小）→ 416。
    # 若放在 end < start 之后判，这种情况会被误降级成全量 200。
    if start >= file_size:
        return UNSATISFIABLE

    if end < start:
        return None

    end = min(end, file_size - 1)
    return ByteRange(start=start, end=end, size=end - start + 1, total=file_size)
