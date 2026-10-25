"""独立存储服务（webx_storage）：鉴权 / 通用对象 / 分片 / 签名 / 任务产物策略 / 清理。

每个用例装配一个全新的应用 + 临时存储目录，互不污染。配置通过替换 webx.config 的全局单例注入，
用例结束自动还原，不影响其它测试用的 app.config.Settings。
"""

import os
import time

import pytest
from sanic import Sanic

import webx.config as webx_config
from webx_storage.config import StorageSettings
from webx_storage.policy import build_policies

TOKEN = "test-token"
H = {"X-Internal-Token": TOKEN}


@pytest.fixture
def make_app(tmp_path, monkeypatch):
    """返回 make(policies=None, **overrides) -> (app, settings)。"""
    created = []

    def make(policies=None, **overrides):
        from webx_storage.app import create_storage_app

        cfg = StorageSettings()
        cfg.STORAGE_DIR = str(tmp_path / "storage")
        cfg.STORAGE_INTERNAL_TOKEN = TOKEN
        cfg.STORAGE_SIGN_SECRET = "sign-secret-for-tests"
        for k, v in overrides.items():
            setattr(cfg, k, v)
        monkeypatch.setattr(webx_config, "_settings", cfg)
        Sanic.test_mode = True
        app = create_storage_app(
            build_policies(["task_results"]) if policies is None else policies,
            name=f"storage_test_{len(created)}_{time.time_ns()}",
        )
        created.append(app)
        return app, cfg

    yield make
    Sanic._app_registry.clear()


@pytest.fixture
def app(make_app):
    return make_app()[0]


def mp(*parts):
    """手工拼 multipart：每项 (name, filename|None, bytes)。"""
    b = "----svc"
    body = b""
    for name, filename, data in parts:
        cd = f'form-data; name="{name}"' + (f'; filename="{filename}"' if filename is not None else "")
        body += f"--{b}\r\nContent-Disposition: {cd}\r\n\r\n".encode() + data + b"\r\n"
    body += f"--{b}--\r\n".encode()
    return body, {"Content-Type": f"multipart/form-data; boundary={b}"}


def post_file(app, path, key=None, filename="f.bin", data=b"hello", headers=None, key_first=False):
    parts = [("file", filename, data)]
    if key is not None:
        kp = ("key", None, key.encode())
        parts = [kp] + parts if key_first else parts + [kp]
    body, ct = mp(*parts)
    return app.test_client.post(path, data=body, headers={**(headers or H), **ct})[1]


# ==================== 鉴权 ====================


@pytest.mark.parametrize("hdr", [{}, {"X-Internal-Token": "wrong"}, {"X-Internal-Token": ""}])
def test_internal_routes_require_the_token(app, hdr):
    for method, path in [
        ("get", "/internal/objects/a/b.txt"),
        ("delete", "/internal/objects/a/b.txt"),
        ("post", "/internal/signatures"),
        ("post", "/internal/uploads"),
        ("delete", "/internal/tasks/t1"),
    ]:
        resp = getattr(app.test_client, method)(path, headers=hdr)[1]
        assert resp.status == 401, (method, path)


def test_unconfigured_token_rejects_everything(make_app):
    """服务端没配令牌时，连"空头"也不能通过——空配置绝不等价于放行。"""
    app, _ = make_app(STORAGE_INTERNAL_TOKEN="")
    for hdr in ({}, {"X-Internal-Token": ""}, {"X-Internal-Token": "anything"}):
        assert app.test_client.get("/internal/objects/a.txt", headers=hdr)[1].status == 401


def test_health_is_public(app):
    assert app.test_client.get("/health")[1].status == 200


# ==================== 通用对象 ====================


@pytest.mark.parametrize("key_first", [True, False])
def test_create_read_delete_object_regardless_of_field_order(app, key_first):
    """表单里 key 排在文件前或后都必须成功——所以内核先写临时文件再落位。"""
    r = post_file(app, "/internal/objects", key="ff/2X/M1/x.chg", data=b"payload", key_first=key_first)
    assert r.status == 201 and r.json["key"] == "ff/2X/M1/x.chg" and r.json["size"] == 7

    got = app.test_client.get("/internal/objects/ff/2X/M1/x.chg", headers=H)[1]
    assert got.status == 200 and got.body == b"payload"

    d1 = app.test_client.delete("/internal/objects/ff/2X/M1/x.chg", headers=H)[1]
    d2 = app.test_client.delete("/internal/objects/ff/2X/M1/x.chg", headers=H)[1]
    assert d1.json["deleted"] is True and d2.status == 200 and d2.json["deleted"] is False, "DELETE 幂等"


def test_object_keys_keep_chinese_names_under_lenient_rule(app):
    r = post_file(app, "/internal/objects", key="t/任务 A_(坐标).zip")
    assert r.status == 201
    from urllib.parse import quote

    # 真实客户端会对路径做百分号编码；服务端必须解码后再按键规则校验
    got = app.test_client.get("/internal/objects/" + quote("t/任务 A_(坐标).zip"), headers=H)[1]
    assert got.status == 200 and got.body == b"hello"


@pytest.mark.parametrize("bad", ["../x", "a/../../x", ".uploads/x", ".staging/x", "a/.hidden", "a\x00b"])
def test_dangerous_keys_are_rejected(app, bad):
    assert post_file(app, "/internal/objects", key=bad).status == 400


def test_missing_key_field_is_400_and_leaves_no_tmp(app, make_app):
    app, cfg = make_app()
    assert post_file(app, "/internal/objects", key=None).status == 400
    leftovers = [p for p in os.listdir(cfg.STORAGE_DIR) if p.startswith(".upload.")]
    assert leftovers == [], "失败的上传不得残留临时文件"


def test_object_range_and_head(app):
    post_file(app, "/internal/objects", key="r/f.bin", data=bytes(range(100)))
    part = app.test_client.get("/internal/objects/r/f.bin", headers={**H, "Range": "bytes=10-19"})[1]
    assert part.status == 206 and part.body == bytes(range(10, 20))
    head = app.test_client.head("/internal/objects/r/f.bin", headers=H)[1]
    assert head.status == 200 and head.headers["Content-Length"] == "100"
    assert app.test_client.get("/internal/objects/none.bin", headers=H)[1].status == 404


def test_delete_object_prunes_empty_directories(make_app):
    app, cfg = make_app()
    post_file(app, "/internal/objects", key="a/b/c/f.txt")
    app.test_client.delete("/internal/objects/a/b/c/f.txt", headers=H)
    assert not os.path.exists(os.path.join(cfg.STORAGE_DIR, "a"))


def test_legacy_object_upload_alias_is_marked_deprecated(app):
    r = post_file(app, "/internal/objects/upload", key="o/legacy.bin")
    assert r.status == 200 and r.json["key"] == "o/legacy.bin"
    assert r.headers["Deprecation"] == "true"
    assert "/internal/objects" in r.headers["Link"]


# ==================== 分片会话 ====================


def new_session(app, key="u/big.bin", total=-1):
    r = app.test_client.post("/internal/uploads", json={"key": key, "total": total}, headers=H)[1]
    assert r.status == 201, r.text
    return r.json["upload_id"]


def test_chunked_session_full_flow_with_offset_headers(app):
    uid = new_session(app, total=8)
    p1 = app.test_client.patch(f"/internal/uploads/{uid}", data=b"abcd", headers={**H, "Upload-Offset": "0"})[1]
    assert p1.status == 200 and p1.headers["Upload-Offset"] == "4" and p1.json["offset"] == 4

    st = app.test_client.get(f"/internal/uploads/{uid}", headers=H)[1]
    assert st.headers["Upload-Offset"] == "4" and st.json["offset"] == 4 and st.json["key"] == "u/big.bin"

    bad = app.test_client.patch(f"/internal/uploads/{uid}", data=b"zz", headers={**H, "Upload-Offset": "9"})[1]
    assert bad.status == 409 and bad.headers["Upload-Offset"] == "4", "冲突时用头带回真实 offset"

    app.test_client.patch(f"/internal/uploads/{uid}", data=b"efgh", headers={**H, "Upload-Offset": "4"})
    done = app.test_client.post(f"/internal/uploads/{uid}/complete", headers=H)[1]
    assert done.status == 200 and done.json["key"] == "u/big.bin" and done.json["size"] == 8
    assert app.test_client.get("/internal/objects/u/big.bin", headers=H)[1].body == b"abcdefgh"


def test_incomplete_session_cannot_complete_and_abort_is_idempotent(app):
    uid = new_session(app, total=100)
    app.test_client.patch(f"/internal/uploads/{uid}", data=b"x", headers={**H, "Upload-Offset": "0"})
    assert app.test_client.post(f"/internal/uploads/{uid}/complete", headers=H)[1].status == 400
    assert app.test_client.delete(f"/internal/uploads/{uid}", headers=H)[1].status == 200
    assert app.test_client.delete(f"/internal/uploads/{uid}", headers=H)[1].status == 200
    assert app.test_client.get(f"/internal/uploads/{uid}", headers=H)[1].status == 404


def test_session_rejects_empty_chunk_missing_offset_and_bad_key(app):
    uid = new_session(app)
    assert app.test_client.patch(f"/internal/uploads/{uid}", data=b"", headers={**H, "Upload-Offset": "0"})[1].status == 400
    assert app.test_client.patch(f"/internal/uploads/{uid}", data=b"x", headers=H)[1].status == 400
    assert app.test_client.post("/internal/uploads", json={"key": "../x"}, headers=H)[1].status == 400
    assert app.test_client.post("/internal/uploads", json={}, headers=H)[1].status == 400


def test_session_tolerates_garbage_total(app):
    """total 是可选的完整性校验，非法入参视为未声明，不能 500。"""
    r = app.test_client.post("/internal/uploads", json={"key": "u/g.bin", "total": "oops"}, headers=H)[1]
    assert r.status == 201 and r.json["total"] == -1


# ==================== 签名 ====================


def test_signature_roundtrip_and_presigning(app):
    sig = app.test_client.post("/internal/signatures", json={"key": "p/f.txt", "ttl_seconds": 60}, headers=H)[1]
    assert sig.status == 200 and sig.json["ttl_seconds"] == 60
    url = sig.json["url"]  # 对象此刻还不存在：允许预签

    assert app.test_client.get(url)[1].status == 404
    post_file(app, "/internal/objects", key="p/f.txt", data=b"signed")
    ok = app.test_client.get(url)[1]
    assert ok.status == 200 and ok.body == b"signed"
    assert app.test_client.head(url)[1].status == 200


def test_signature_is_bound_to_its_key_and_invalid_is_404(app):
    post_file(app, "/internal/objects", key="p/a.txt")
    post_file(app, "/internal/objects", key="p/b.txt")
    sign = app.test_client.post("/internal/signatures", json={"key": "p/a.txt"}, headers=H)[1].json["sign"]
    assert app.test_client.get(f"/public/objects/p/b.txt?sign={sign}")[1].status == 404, "签名不能挪用到别的对象"
    assert app.test_client.get("/public/objects/p/a.txt?sign=garbage")[1].status == 404
    assert app.test_client.get("/public/objects/p/a.txt")[1].status == 404, "通用对象的公开下载必须带签名"


def test_signature_for_illegal_key_is_400(app):
    assert app.test_client.post("/internal/signatures", json={"key": "../x"}, headers=H)[1].status == 400
    assert app.test_client.post("/internal/signatures", json={}, headers=H)[1].status == 400


# ==================== 任务产物策略（规范路径）====================


def result_url(task, name):
    return f"/public/tasks/{task}/results/{name}"


def test_create_task_result_returns_download_url(app):
    r = post_file(app, "/internal/tasks/T1/results", filename="em.zip", data=b"zipdata")
    assert r.status == 201
    assert r.json == {
        "task_id": "T1", "filename": "em.zip", "size": 7, "download_url": result_url("T1", "em.zip"),
    }
    # 键布局是策略的私有知识：通用接口也能按布局读到
    assert app.test_client.get("/internal/objects/tasks/T1/results/em.zip", headers=H)[1].body == b"zipdata"


def test_form_filename_overrides_part_filename_and_traversal_is_flattened(app):
    body, ct = mp(("filename", None, "renamed.bin".encode()), ("file", "orig.bin", b"x"))
    r = app.test_client.post("/internal/tasks/T2/results", data=body, headers={**H, **ct})[1]
    assert r.json["filename"] == "renamed.bin"

    body, ct = mp(("file", "../../evil.bin", b"x"))
    r = app.test_client.post("/internal/tasks/T2/results", data=body, headers={**H, **ct})[1]
    assert r.status == 201 and r.json["filename"] == "evil.bin", "路径成分被砍掉，不会写到任务目录之外"


@pytest.mark.parametrize("bad_task", ["a.b", "a b", "..", "a;b"])
def test_illegal_task_id_is_rejected(app, bad_task):
    assert post_file(app, f"/internal/tasks/{bad_task}/results").status in (400, 404)


def test_public_download_is_open_by_default_and_signed_when_requested(app):
    post_file(app, "/internal/tasks/T3/results", filename="a.bin", data=b"data")
    assert app.test_client.get(result_url("T3", "a.bin"))[1].body == b"data", "默认公开"

    sig = app.test_client.post("/internal/tasks/T3/signatures", json={"filename": "a.bin"}, headers=H)[1]
    assert sig.status == 200 and sig.json["url"].startswith(result_url("T3", "a.bin") + "?sign=")
    assert app.test_client.get(sig.json["url"])[1].status == 200

    bad = app.test_client.get(result_url("T3", "a.bin") + "?sign=garbage")[1]
    assert bad.status == 403, "携带了签名就必须校验，无效 → 403"
    assert app.test_client.get(result_url("T3", "none.bin"))[1].status == 404


def test_signature_cannot_be_moved_to_another_result(app):
    post_file(app, "/internal/tasks/T4/results", filename="a.bin")
    post_file(app, "/internal/tasks/T4/results", filename="b.bin")
    sign = app.test_client.post("/internal/tasks/T4/signatures", json={"filename": "a.bin"}, headers=H)[1].json["sign"]
    assert app.test_client.get(result_url("T4", "b.bin") + f"?sign={sign}")[1].status == 403


def test_result_download_range_head_and_download_name(app):
    post_file(app, "/internal/tasks/T5/results", filename="d.bin", data=bytes(range(50)))
    part = app.test_client.get(result_url("T5", "d.bin"), headers={"Range": "bytes=5-9"})[1]
    assert part.status == 206 and part.body == bytes(range(5, 10))
    assert app.test_client.head(result_url("T5", "d.bin"))[1].headers["Content-Length"] == "50"

    named = app.test_client.get(result_url("T5", "d.bin") + "?name=" + "任务<>x")[1]
    assert "filename*=UTF-8''" in named.headers["Content-Disposition"]
    assert "T5_d.bin" in app.test_client.get(result_url("T5", "d.bin"))[1].headers["Content-Disposition"], "缺省用 task_id 作前缀"


def test_log_results_are_served_inline_as_text(app):
    post_file(app, "/internal/tasks/T6/results", filename="run.log", data="第一行\n第二行".encode())
    r = app.test_client.get(result_url("T6", "run.log"))[1]
    assert r.status == 200 and r.headers["Content-Disposition"] == "inline" and "第二行" in r.text


def test_list_results_and_delete_are_idempotent(app):
    for n in ("1.bin", "2.bin"):
        post_file(app, "/internal/tasks/T7/results", filename=n)
    assert app.test_client.get("/public/tasks/T7/results")[1].json["files"] == ["1.bin", "2.bin"]

    d1 = app.test_client.delete("/internal/tasks/T7/results/1.bin", headers=H)[1]
    d2 = app.test_client.delete("/internal/tasks/T7/results/1.bin", headers=H)[1]
    assert d1.json["deleted"] is True and d2.status == 200 and d2.json["deleted"] is False

    t1 = app.test_client.delete("/internal/tasks/T7", headers=H)[1]
    t2 = app.test_client.delete("/internal/tasks/T7", headers=H)[1]
    assert t1.json["deleted"] is True and t2.status == 200 and t2.json["deleted"] is False
    assert app.test_client.get("/public/tasks/T7/results")[1].json["files"] == []


def test_task_result_session_flow_returns_download_url_on_complete(app):
    s = app.test_client.post(
        "/internal/tasks/T8/results/uploads", json={"filename": "big.zip", "total": 6}, headers=H
    )[1]
    assert s.status == 201 and s.json["task_id"] == "T8" and s.json["filename"] == "big.zip"
    uid = s.json["upload_id"]
    app.test_client.patch(f"/internal/uploads/{uid}", data=b"abcdef", headers={**H, "Upload-Offset": "0"})
    done = app.test_client.post(f"/internal/uploads/{uid}/complete", headers=H)[1]
    assert done.json["download_url"] == result_url("T8", "big.zip") and done.json["size"] == 6
    assert app.test_client.get(result_url("T8", "big.zip"))[1].body == b"abcdef"
    assert app.test_client.post("/internal/tasks/T8/results/uploads", json={}, headers=H)[1].status == 400


def test_batch_deletions_are_isolated_and_report_one_to_one(app):
    for n in ("a.zip", "b.zip"):
        post_file(app, "/internal/tasks/T9/results", filename=n)
    post_file(app, "/internal/tasks/OTHER/results", filename="a.zip")
    urls = [
        result_url("T9", "a.zip"),
        "https://cdn.example/prefix" + result_url("T9", "b.zip") + "?sign=x",  # 反代前缀与查询串不影响反解
        result_url("T9", "a.zip"),                                              # 重复项
        result_url("OTHER", "a.zip"),                                           # 别的任务：不许跨任务删
        "/public/objects/ff/x.chg",                                             # 不属于任务布局
    ]
    r = app.test_client.post("/internal/tasks/T9/results/deletions", json={"urls": urls}, headers=H)[1]
    assert r.status == 200 and r.json["requested"] == 5 and r.json["deleted"] == 2
    reasons = [i.get("reason") for i in r.json["items"]]
    assert reasons == [None, None, "duplicate", "foreign_task", "not_a_result_url"]
    assert app.test_client.get(result_url("OTHER", "a.zip"))[1].status == 200, "别的任务的文件必须还在"


def test_batch_deletions_validate_the_body(app):
    url = "/internal/tasks/T9/results/deletions"
    assert app.test_client.post(url, json=["x"], headers=H)[1].status == 400
    assert app.test_client.post(url, json={"urls": "x"}, headers=H)[1].status == 400
    assert app.test_client.post(url, json={"urls": ["u"] * 501}, headers=H)[1].status == 400


# ==================== 兼容别名（已废弃）====================


def test_legacy_aliases_keep_the_old_contract_and_are_marked_deprecated(app):
    up = post_file(app, "/internal/tasks/L1/results/upload", filename="x.bin", data=b"12345")
    assert up.status == 200, "旧契约是 200，不是 201"
    assert up.json == {"task_id": "L1", "filename": "x.bin", "download_url": result_url("L1", "x.bin"), "size": 5}
    assert up.headers["Deprecation"] == "true" and "/internal/tasks/{task_id}/results" in up.headers["Link"]

    init = app.test_client.post("/internal/tasks/L1/results/upload/init", json={"filename": "y.bin"}, headers=H)[1]
    assert init.status == 200 and init.json["offset"] == 0 and init.headers["Deprecation"] == "true"

    sign = app.test_client.post("/internal/sign", json={"task_id": "L1", "filename": "x.bin", "ttl_seconds": 30}, headers=H)[1]
    assert sign.status == 200 and {"object_key", "sign", "url", "ttl_seconds"} <= set(sign.json)
    assert sign.json["object_key"] == "tasks/L1/results/x.bin" and sign.json["ttl_seconds"] == 30
    assert app.test_client.get(sign.json["url"])[1].status == 200
    assert app.test_client.post("/internal/sign", json={"task_id": "L1"}, headers=H)[1].status == 400

    dele = app.test_client.post("/internal/tasks/L1/results/deletes", json={"urls": [result_url("L1", "x.bin")]}, headers=H)[1]
    assert dele.json["deleted"] == 1 and dele.headers["Deprecation"] == "true"


def test_canonical_routes_are_not_marked_deprecated(app):
    r = post_file(app, "/internal/tasks/C1/results", filename="x.bin")
    assert "Deprecation" not in r.headers


# ==================== 策略装配 ====================


def test_unknown_policy_name_fails_at_startup():
    with pytest.raises(ValueError, match="未知的存储键策略"):
        build_policies(["typo_policy"])


def test_without_policies_only_the_generic_api_exists(make_app):
    app, _ = make_app(policies=[])
    assert app.test_client.post("/internal/tasks/T/results", headers=H)[1].status == 404
    assert app.test_client.get("/public/tasks/T/results")[1].status == 404
    assert post_file(app, "/internal/objects", key="a/b.txt").status == 201


def test_custom_policy_can_add_routes_and_describe_keys(make_app):
    """策略是真正的扩展点：不改内核就能挂自己的路由、给键补响应字段。"""
    from sanic.response import json

    from webx_storage.policy import KeyPolicy

    class Avatars(KeyPolicy):
        name = "avatars"

        def mount(self, ctx):
            @ctx.public.get("/avatars/<uid:str>")
            async def avatar(request, uid):
                return json({"uid": uid})

        def describe_object(self, key, size):
            return {"avatar": True} if key.startswith("avatars/") else {}

    app, _ = make_app(policies=[Avatars()])
    assert app.test_client.get("/public/avatars/u1")[1].json == {"uid": "u1"}
    r = post_file(app, "/internal/objects", key="avatars/u1.png")
    assert r.json["avatar"] is True
    assert "avatar" not in post_file(app, "/internal/objects", key="misc/u1.png").json


# ==================== 启动 ====================


def test_startup_reclaims_stale_upload_sessions(make_app):
    import json as _json

    app, cfg = make_app(UPLOAD_SESSION_MAX_AGE_HOURS=1.0)
    uid = new_session(app)
    meta = os.path.join(cfg.STORAGE_DIR, ".uploads", uid, "meta.json")
    data = _json.load(open(meta))
    data["created_ms"] = int((time.time() - 5 * 3600) * 1000)
    _json.dump(data, open(meta, "w"))

    app.test_client.get("/health")  # 触发 before_server_start 钩子
    assert not os.path.exists(os.path.dirname(meta)), "超龄会话必须在启动时被回收"


# ==================== 保留期清理 ====================


def _make_task_dir(root, task_id, age_days, files=("a.bin",)):
    d = root / "tasks" / task_id / "results"
    d.mkdir(parents=True)
    ts = time.time() - age_days * 86400
    for f in files:
        p = d / f
        p.write_bytes(b"x")
        os.utime(p, (ts, ts))
    for p in (d, d.parent):
        os.utime(p, (ts, ts))
    return d.parent


@pytest.fixture
def retention_store(tmp_path):
    from webx.lib.storage import ObjectStore

    s = ObjectStore(tmp_path / "s", key_rule="lenient")
    s.ensure_dirs()
    return s


def test_sweep_removes_only_expired_units_inside_its_scope(retention_store):
    from webx_storage.retention import MtimeRetention, sweep

    root = retention_store.root
    _make_task_dir(root, "old", age_days=100)
    _make_task_dir(root, "fresh", age_days=1)
    # 范围之外的数据即使很旧也绝不能碰（它们的生命周期归各自的业务方）
    retention_store.save_bytes("objects/ff/keep.chg", b"x")
    os.utime(root / "objects" / "ff" / "keep.chg", (1, 1))

    checked, removed = sweep(retention_store, MtimeRetention("tasks", days=90))
    assert (checked, removed) == (2, 1)
    assert not (root / "tasks" / "old").exists() and (root / "tasks" / "fresh").exists()
    assert (root / "objects" / "ff" / "keep.chg").exists()


def test_a_recently_touched_file_keeps_the_whole_unit_alive(retention_store):
    """判据是整棵树里最新的 mtime：还在被追加产物的任务不该被删。"""
    from webx_storage.retention import MtimeRetention, sweep

    unit = _make_task_dir(retention_store.root, "busy", age_days=200, files=("old.bin", "new.bin"))
    now = time.time()
    os.utime(unit / "results" / "new.bin", (now, now))
    assert sweep(retention_store, MtimeRetention("tasks", days=90)) == (1, 0)
    assert unit.exists()


def test_dry_run_deletes_nothing_and_cap_limits_each_round(retention_store):
    from webx_storage.retention import MtimeRetention, sweep

    for i in range(4):
        _make_task_dir(retention_store.root, f"old{i}", age_days=100)
    pol = MtimeRetention("tasks", days=90)
    assert sweep(retention_store, pol, dry_run=True) == (4, 4)
    assert len(list((retention_store.root / "tasks").iterdir())) == 4
    assert sweep(retention_store, pol, max_per_round=2)[1] == 2
    assert len(list((retention_store.root / "tasks").iterdir())) == 2, "上限之外的留待下一轮"


def test_confirm_hook_can_veto_deletion(retention_store):
    """权威方不可达 / 不同意 → 不删。这是删除不可逆所要求的底线。"""
    from webx_storage.retention import MtimeRetention, sweep

    class Veto(MtimeRetention):
        def confirm(self, c):
            return False

    _make_task_dir(retention_store.root, "old", age_days=100)
    assert sweep(retention_store, Veto("tasks", 90)) == (1, 0)
    assert (retention_store.root / "tasks" / "old").exists()


def test_a_failing_unit_does_not_abort_the_round(retention_store):
    from webx_storage.retention import MtimeRetention, sweep

    class Flaky(MtimeRetention):
        def remove(self, store, c):
            if c.id == "bad":
                raise OSError("disk error")
            return super().remove(store, c)

    _make_task_dir(retention_store.root, "bad", age_days=100)
    _make_task_dir(retention_store.root, "good", age_days=100)
    assert sweep(retention_store, Flaky("tasks", 90)) == (2, 1)


def test_retention_process_refuses_to_run_unless_enabled(make_app, capsys):
    from webx_storage import retention

    _, cfg = make_app(STORAGE_RETENTION_ENABLED=False)
    # main() 会重新 configure()，这里让它读到"关闭"的环境
    os.environ["STORAGE_RETENTION_ENABLED"] = "false"
    try:
        assert retention.main([]) == 0
    finally:
        os.environ.pop("STORAGE_RETENTION_ENABLED", None)
    assert "未开启" in capsys.readouterr().out


# ==================== 命名空间隔离 / 键深度 / 反解 ====================


def test_generic_api_cannot_write_or_delete_inside_a_policy_namespace(app):
    """通用对象写进 tasks/ 会被任务产物的清理当成自己的东西删掉；反向也不许毁策略数据。"""
    assert post_file(app, "/internal/objects", key="tasks/T/results/forged.bin").status == 403
    assert app.test_client.post("/internal/uploads", json={"key": "tasks/T/results/f.bin"}, headers=H)[1].status == 403

    post_file(app, "/internal/tasks/T/results", filename="real.bin")
    assert app.test_client.delete("/internal/objects/tasks/T/results/real.bin", headers=H)[1].status == 403
    assert app.test_client.get(result_url("T", "real.bin"))[1].status == 200, "数据没被毁"
    # 读取不受限：内部服务本就能读全部
    assert app.test_client.get("/internal/objects/tasks/T/results/real.bin", headers=H)[1].status == 200


def test_without_the_policy_the_namespace_is_not_reserved(make_app):
    app, _ = make_app(policies=[])
    assert post_file(app, "/internal/objects", key="tasks/x.bin").status == 201


def test_key_depth_is_capped(app):
    assert post_file(app, "/internal/objects", key="/".join(["d"] * 16) + "/f").status == 400
    assert post_file(app, "/internal/objects", key="/".join(["d"] * 8) + "/f").status == 201


@pytest.mark.parametrize(
    "url, expected",
    [
        ("/public/tasks/T/results/a.zip", ("T", "a.zip")),
        ("https://h.example/prefix/public/tasks/T/results/a.zip?sign=abc#frag", ("T", "a.zip")),
        ("/public/tasks/T/results/%E4%BB%BB%E5%8A%A1.zip", ("T", "任务.zip")),
        ("/public/objects/ff/x.chg", None),
        ("/public/tasks/T/other/a.zip", None),
        ("", None),
        (None, None),
        (123, None),
    ],
)
def test_parse_url_forms(url, expected):
    from webx_storage.task_results import TaskResultsPolicy

    assert TaskResultsPolicy.parse_url(url) == expected


def test_empty_batch_is_a_noop(app):
    r = app.test_client.post("/internal/tasks/T/results/deletions", json={"urls": []}, headers=H)[1]
    assert r.status == 200 and r.json["requested"] == 0 and r.json["deleted"] == 0


def test_sweep_ignores_stray_files_and_empty_storage(retention_store):
    from webx_storage.retention import MtimeRetention, sweep

    pol = MtimeRetention("tasks", days=90)
    assert sweep(retention_store, pol) == (0, 0), "没有 tasks 目录也不报错"
    (retention_store.root / "tasks").mkdir()
    (retention_store.root / "tasks" / "stray.txt").write_text("x")
    assert sweep(retention_store, pol) == (0, 0), "tasks 下的零散文件不是清理单元"


def test_retention_boundary(retention_store):
    from webx_storage.retention import MtimeRetention, sweep

    _make_task_dir(retention_store.root, "inside", age_days=89)
    _make_task_dir(retention_store.root, "outside", age_days=91)
    assert sweep(retention_store, MtimeRetention("tasks", days=90)) == (2, 1)
    assert (retention_store.root / "tasks" / "inside").exists()
