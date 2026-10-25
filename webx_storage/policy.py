"""键策略：把「业务形状」与「通用对象存储」隔开的唯一接缝。

内核只认**对象键**。策略负责业务形状那一侧：
  - 在内核路由之上追加自己的路由（如 ``/internal/tasks/<id>/results``）
  - 为键补充响应字段（如 ``download_url``）
  - 声明自己的保留期清理方式（可选）

这样存储服务本身不知道什么是任务，换一种业务形状只需要写一个策略。
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, ClassVar, Optional

from sanic import Blueprint

from webx.services.storage_service import StorageService

if TYPE_CHECKING:  # 仅类型检查用，避免与 retention 互相导入
    from webx_storage.retention import RetentionPolicy

logger = logging.getLogger(__name__)


@dataclass
class StorageContext:
    """一次装配的共享上下文：存储门面 + 两个蓝图 + 已启用的策略。

    每个应用一份。蓝图在这里创建而不是模块级——模块级蓝图是进程单例，
    一个进程就只能装配一个应用，测试与多实例都会被卡住。
    """

    svc: StorageService
    internal: Blueprint
    public: Blueprint
    policies: list["KeyPolicy"] = field(default_factory=list)

    def owner_of(self, key: str) -> Optional["KeyPolicy"]:
        """返回拥有该键所在命名空间的策略；无人拥有返回 None。"""
        for policy in self.policies:
            if policy.owns(key):
                return policy
        return None

    def check_generic_write(self, key: str) -> None:
        """通用接口写 / 删键之前的闸：策略拥有的命名空间只能走该策略的专属接口。

        为什么：命名空间的生命周期规则（保留期清理、布局约定）归拥有它的策略。
        通用接口若能随便写进去，写进去的对象就会被该策略的清理当成自己的产物删掉；
        反过来通用接口删键也可能毁掉策略的数据。读取不受限（内部服务本就能读全部）。
        """
        from webx.lib.storage import StoreError

        owner = self.owner_of(key)
        if owner is not None:
            raise StoreError(
                f"键 {key!r} 属于 {owner.name} 策略的命名空间，请使用该策略的专属接口", status=403
            )

    def describe(self, key: str, size: int) -> dict:
        """汇总各策略为该键补充的响应字段（如 download_url）。"""
        extra: dict = {}
        for policy in self.policies:
            extra.update(policy.describe_object(key, size))
        return extra


class KeyPolicy(ABC):
    """键策略基类。"""

    #: 策略名，对应 ``STORAGE_POLICIES`` 配置值
    name: ClassVar[str]

    @abstractmethod
    def mount(self, ctx: StorageContext) -> None:
        """把本策略的路由挂到 ``ctx.internal`` / ``ctx.public``。"""

    def describe_object(self, key: str, size: int) -> dict:
        """为一个已落位的对象键补充响应字段；不认识该键就返回空 dict。"""
        return {}

    def owns(self, key: str) -> bool:
        """该键是否位于本策略独占的命名空间。默认不独占任何命名空间。"""
        return False

    def retention(self) -> Optional["RetentionPolicy"]:
        """本策略的保留期清理方式；不需要清理返回 None。"""
        return None


_POLICIES: dict[str, Callable[[], KeyPolicy]] = {}


def register_policy(name: str, factory: Callable[[], KeyPolicy]) -> None:
    """注册策略工厂（重复注册即覆盖）。"""
    _POLICIES[name] = factory


def build_policies(names: list[str]) -> list[KeyPolicy]:
    """按名字实例化策略。**未知名字直接报错**：

    策略决定了对外暴露哪些路由，拼错名字就悄悄少一组路由或不清理，比启动失败更难排查。
    """
    out: list[KeyPolicy] = []
    for name in names:
        name = name.strip()
        if not name:
            continue
        if name not in _POLICIES:
            raise ValueError(f"未知的存储键策略 {name!r}，可选: {', '.join(sorted(_POLICIES))}")
        out.append(_POLICIES[name]())
    return out
