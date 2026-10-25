"""挂起池——让领取请求在无任务时休眠，有新任务时被唤醒。

## 这里持有的是"连接"，不是"数据"

本服务**不缓存任何业务数据**。挂起的领取请求醒来后一律回数据库重新认领；poke 的消息体是空的，
它只说"有事了，去看看"，从不携带任务内容。

理由是权威方向：任务归属的权威在数据库，谁都不该在旁边留一份会过期的镜像。单进程的价值不是"快"，
而是**挂起池集中在一处**（Web 多开时 poke 只需送到一个地方），以及只有一处需要推敲时序。

## 竞态：必须"先取代次，再查库"

反过来写会丢唤醒::

    ✗  查库(空) → [此刻 poke 到达] → 取事件 → 等待 → 直到兜底超时才发现有活
    ✓  取事件   → 查库(空) → 等待     → 手上的事件已被 poke 置位，wait 立即返回

所以 ``current()`` 必须在认领尝试**之前**调用。``poke()`` 置位后立刻换一个新 Event，
避免已醒的等待者在下一轮拿到一个"天生已置位"的旧事件而空转。
"""

from __future__ import annotations

import asyncio


class PokeSignal:
    """一次性广播信号（代次事件）。

    用 Event 而非 Condition / Queue：唤醒语义是**广播**（有新任务时所有空闲 worker 都该去抢），
    而不是点名。抢不到的自己回去接着等，互斥由数据库保证。
    """

    __slots__ = ("_event", "_pokes")

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._pokes = 0

    def current(self) -> asyncio.Event:
        """取当前代次的事件。**必须在查库之前调用**（见模块 docstring 的竞态说明）。"""
        return self._event

    def poke(self) -> None:
        """广播"有新任务"。幂等、无副作用、不阻塞。"""
        self._pokes += 1
        self._event.set()
        # 换代：已被唤醒的等待者下一轮会取到这个新的（未置位）事件
        self._event = asyncio.Event()

    @property
    def poke_count(self) -> int:
        return self._pokes


async def wait_for_poke(event: asyncio.Event, timeout: float) -> bool:
    """等待一次唤醒。

    Args:
        event:   由 ``PokeSignal.current()`` 在**查库之前**取得的事件
        timeout: 最长等待秒数

    Returns:
        True=被 poke 唤醒；False=超时（该回去做一次兜底查询）
    """
    if timeout <= 0:
        return False
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


#: 进程内单例：本服务是**单进程**的（挂起池必须集中在一处），一个全局实例足够
poke_signal = PokeSignal()
