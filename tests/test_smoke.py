"""冒烟测试：应用装配与请求链路的端到端行为。

覆盖点：
  - 项目根定位与 .env 加载
  - 蓝图自动扫描 + 中间件层级继承（根级链条 + api 目录追加认证）
  - 令牌签发/校验闭环
  - 统一异常处理器按 status/code 渲染
  - i18n 按请求头切换语种

运行：``.venv/bin/python -m pytest tests/ -v``
"""

# 路径、配置与文案的初始化统一在 conftest 完成（仓库根即项目根）

import pytest

from webx.lib.token import get_token


@pytest.fixture(scope="module")
def test_app(app_with_db):
    """复用会话级应用（一个进程只能装配一个，见 conftest.app_with_db）。"""
    return app_with_db


def test_health(test_app):
    _, resp = test_app.test_client.get("/health")
    assert resp.status == 200
    assert resp.text == "ok"


def test_index_i18n(test_app):
    _, zh = test_app.test_client.get("/")
    assert zh.json["message"] == "操作成功"

    _, en = test_app.test_client.get("/", headers={"X-Lang": "en"})
    assert en.json["message"] == "Success"


def test_public_route_and_middleware_chain(test_app):
    """公开路由：无需令牌，且根级中间件已注入 lang / request_id。"""
    _, resp = test_app.test_client.get("/public/welcome")
    assert resp.status == 200
    assert resp.json["message"] == "hello webx"
    assert resp.json["lang"] == "zh"
    assert resp.json["request_id"]  # request_id 中间件已生效


def test_api_requires_auth(test_app):
    """API 目录追加了 auth_middleware：无令牌一律 401。"""
    _, resp = test_app.test_client.get("/api/notes")
    assert resp.status == 401

    _, bad = test_app.test_client.get(
        "/api/notes", headers={"Authorization": "Bearer not-a-real-token"}
    )
    assert bad.status == 401


def test_token_roundtrip():
    """令牌签发 → 校验闭环，payload 原样返回。"""
    xt = get_token()
    token = xt.make_token({"user_id": "u1", "username": "demo", "role": "user"}, id_="user")
    result = xt.verify_token(token)
    assert result["ok"] is True
    assert result["data"]["user_id"] == "u1"
    assert result["data"]["role"] == "user"


def test_valid_token_passes_auth_middleware(test_app):
    """有效令牌能通过 API 目录的认证中间件。

    这里直接签发令牌，不经登录端点——本用例要验的是**中间件链**，
    不该被登录逻辑的变化牵连（完整的登录链路见 test_http_e2e）。
    """
    token = get_token().make_token(
        {"user_id": "u_smoke", "username": "smoke", "role": "user"}, id_="user"
    )

    # 认证通过后进入 handler；handler 要查库，故只断言"不是 401"
    _, resp = test_app.test_client.get(
        "/api/notes", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status != 401


def test_role_gate_blocks_non_admin(test_app):
    """角色闸门：user 角色的令牌打不开 admin 中间件。"""
    from webx.middleware import make_auth_middleware

    from sanic import Blueprint
    from sanic.response import json as sjson

    # 挂在蓝图上而非全局：与 __mid__.py 的真实用法一致，也不会污染其他用例
    bp = Blueprint("admin_only", url_prefix="/only-admin")
    bp.middleware(make_auth_middleware(required_roles=["admin"]))

    @bp.get("")
    async def only_admin(request):
        return sjson({"ok": True})

    test_app.blueprint(bp)

    xt = get_token()
    user_token = xt.make_token({"user_id": "u1", "role": "user"}, id_="user")
    _, resp = test_app.test_client.get(
        "/only-admin", headers={"Authorization": f"Bearer {user_token}"}
    )
    assert resp.status == 403


def test_error_handler_renders_code(test_app):
    """WebxError 抛出即按 status/code 渲染，不需要在入口写处理器。"""
    from webx.core.errors import NotFoundError

    @test_app.get("/boom", name="boom")
    async def boom(request):
        raise NotFoundError(code="note_not_found")

    _, resp = test_app.test_client.get("/boom")
    assert resp.status == 404
    assert resp.json["code"] == "note_not_found"
    assert resp.json["error"] == "笔记不存在"


# ==================== 内置路由开关 ====================


def test_builtin_groups_can_be_selected():
    """``builtin_controller`` 可只挂子集——这是改写某一组的前提。"""
    from webx.core.app import BUILTIN_GROUPS, _builtin_packages

    assert _builtin_packages(True) == ["webx.controller"]
    assert _builtin_packages(False) == []
    assert _builtin_packages(["auth", "admin"]) == [
        "webx.controller.auth",
        "webx.controller.admin",
    ]
    assert set(BUILTIN_GROUPS) == {"auth", "api", "admin", "public"}


def test_unknown_builtin_group_is_rejected():
    """写错组名要当场报错，而不是静默少挂一组路由。"""
    from webx.core.app import _builtin_packages

    with pytest.raises(ValueError, match="未知的内置路由组"):
        _builtin_packages(["nope"])
