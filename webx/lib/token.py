"""
WebxToken —— AES-GCM/CBC 加密令牌（零第三方依赖）

加解密原语已抽离到 `_aes_backend`：native(EVP/libcrypto) 优先 + 纯 Python 兜底，
两者算法格式 100% 一致（同一 AES），由后端 import 期双向对拍保证。本模块只负责
令牌的封装/校验/ID 管理/刷新等上层逻辑，与原实现完全等价。

令牌格式：``{riv}.{ciphertext}`` —— riv 为每次签发随机生成的 IV，随 token 明文
携带，配合固定 key 解密。
"""

import json
import time
import base64
import secrets
from typing import Optional

# AES 原语后端（EVP 优先 / 纯 Python 兜底，签名与旧实现完全一致）
from webx.lib._aes_backend import (
    BACKEND as AES_BACKEND,
    aes_gcm_encrypt,
    aes_gcm_decrypt,
    aes_cbc_encrypt,
    aes_cbc_decrypt,
)


def randstring(n: int) -> str:
    """生成指定长度的随机字母数字字符串"""
    alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(secrets.choice(alphabet) for _ in range(n))


def b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def b64url_decode(s: str) -> bytes:
    padding = 4 - len(s) % 4
    if padding != 4:
        s += '=' * padding
    return base64.urlsafe_b64decode(s)


class WebxToken:

    def __init__(self, options: dict = None):
        if options is None:
            options = {}

        # 默认算法
        self.algorithm = 'aes-256-gcm'
        self.iv = randstring(12)
        self.is_gcm = True
        self.iv_length = 12
        self.key = randstring(32)
        self.token_encoding = 'base64url'
        self.expires = 10800000       # 3小时（毫秒）
        self.refresh = 0
        self.failed_code = 401

        self.token_map: dict[str, object] = {}
        self.id_key_iv: dict[str, dict] = {}
        self.token_ids: list[str] = []
        self.id_index = -1

        self.token_tag = 'webx-token'
        self.token_start_index = len(self.token_tag) + 1

        if not isinstance(options, dict):
            options = {}

        for k, v in options.items():
            if k == 'iv':
                self.iv = v
            elif k == 'key':
                self.key = v
            elif k == 'expires':
                if isinstance(v, (int, float)) and v > 0:
                    self.expires = int(v * 1000)
            elif k == 'encoding':
                self.token_encoding = v
            elif k in ('alg', 'algorithm'):
                valid_algs = [
                    'aes-256-gcm', 'aes-192-gcm', 'aes-128-cbc',
                    'aes-192-cbc', 'aes-256-cbc', 'sm4-cbc', 'sm4'
                ]
                alg = v.lower()
                if alg in valid_algs:
                    self.algorithm = alg
                    if '-gcm' not in self.algorithm:
                        self.iv = randstring(16)
                        self.is_gcm = False
                        self.iv_length = 16
                if self.algorithm == 'sm4':
                    self.algorithm = 'sm4-cbc'  # SM4 暂不支持，保留字段
            elif k == 'failedCode':
                if isinstance(v, int) and 400 <= v <= 499:
                    self.failed_code = v

        self.key = self.fix_key(self.key)
        self.iv = self.fix_iv(self.iv)

    # ── 加解密核心 ────────────────────────────────────────────────

    def _get_key_bytes(self, key) -> bytes:
        if isinstance(key, str):
            return key.encode('utf-8')
        return key

    def _get_iv_bytes(self, iv) -> bytes:
        if isinstance(iv, str):
            return iv.encode('utf-8')
        return iv

    def aes_encrypt(self, data: str, key, iv) -> bytes:
        key_b = self._get_key_bytes(key)
        iv_b = self._get_iv_bytes(iv)
        plaintext = data.encode('utf-8')

        if self.is_gcm:
            ciphertext, tag = aes_gcm_encrypt(key_b, iv_b, plaintext)
            return ciphertext + tag
        else:
            return aes_cbc_encrypt(key_b, iv_b, plaintext)

    def aes_decrypt(self, edata: str, key, iv) -> bytes:
        key_b = self._get_key_bytes(key)
        iv_b = self._get_iv_bytes(iv)

        if self.is_gcm:
            raw = b64url_decode(edata)
            tag = raw[-16:]
            ciphertext = raw[:-16]
            return aes_gcm_decrypt(key_b, iv_b, ciphertext, tag)
        else:
            raw = b64url_decode(edata)
            return aes_cbc_decrypt(key_b, iv_b, raw)

    # ── Key / IV 修正 ─────────────────────────────────────────────

    def fix_key(self, key: str) -> str:
        if len(key) < 16:
            raise ValueError("key.length must >= 16")

        alg = self.algorithm
        if alg.startswith('sm4') or alg.startswith('aes-128'):
            real_len = 16
        elif alg.startswith('aes-192'):
            real_len = 24
        else:
            real_len = 32

        if len(key) == real_len:
            return key
        if len(key) > real_len:
            return key[:real_len]

        new_key = key + key
        if len(new_key) == real_len:
            return new_key
        return new_key[:real_len]

    def fix_iv(self, iv: str) -> str:
        leng = len(iv)
        if leng == self.iv_length:
            return iv
        if leng > self.iv_length:
            return iv[:self.iv_length]
        suffix = ''.join(str(i) for i in range(self.iv_length - leng, 0, -1))
        return iv + suffix

    # ── Token ID 管理 ─────────────────────────────────────────────

    def add_token_id(self, tid):
        if isinstance(tid, str):
            tid = [tid]

        if isinstance(tid, dict):
            for k, v in tid.items():
                if k not in self.token_ids:
                    self.token_ids.append(k)
                    self.set_map_id(k, v)
        elif isinstance(tid, list):
            for item in tid:
                if item not in self.token_ids:
                    self.token_ids.append(item)
                    self.token_map[item] = len(self.token_ids)

    def set_map_id(self, tid: str, iv=None):
        self.token_map[tid] = iv if iv is not None else tid

    def set_id_key_iv(self, id_: str, key: str, iv: str):
        key = self.fix_key(key)
        iv = self.fix_iv(iv)
        self.add_token_id(id_)
        self.id_key_iv[id_] = {'key': key, 'iv': iv, 'id': id_}

    def has_id(self, tid: str) -> bool:
        return tid in self.token_map

    def remove_token_id(self, tid: str):
        if tid in self.token_ids:
            self.token_ids.remove(tid)
        self.token_map.pop(tid, None)
        self.id_key_iv.pop(tid, None)

    def rand_id(self) -> str:
        if not self.token_ids:
            return ''
        return secrets.choice(self.token_ids)

    def step_id(self) -> str:
        if not self.token_ids:
            return ''
        if self.id_index >= len(self.token_ids) - 1:
            self.id_index = -1
        self.id_index += 1
        return self.token_ids[self.id_index]

    # ── Setter ─────────────────────────────────────────────────────

    def set_iv(self, iv: str):
        self.iv = self.fix_iv(iv)

    def set_key(self, key: str):
        self.key = self.fix_key(key)

    def set_encoding(self, ecode: str):
        self.token_encoding = ecode

    def set_expires(self, expires: int):
        self.expires = expires

    def set_refresh(self, flag: bool = True):
        self.refresh = self.expires // 5 if flag else 0

    # ── Token 生成 ────────────────────────────────────────────────

    def _encode(self, data: bytes) -> str:
        return b64url_encode(data)

    def make(self, userinfo: dict, token_id: str = None) -> str:
        if not userinfo.get('expires') or not isinstance(userinfo.get('expires'), int):
            userinfo['expires'] = self.expires
        userinfo['timestamp'] = int(time.time() * 1000)
        userinfo['__tokenId__'] = token_id or self.step_id()

        ikv = self.id_key_iv.get(token_id) if token_id else None
        if token_id and ikv:
            tk = self.aes_encrypt(json.dumps(userinfo), ikv['key'], ikv['iv'])
        else:
            tk = self.aes_encrypt(json.dumps(userinfo), self.key, self.iv)
        return self._encode(tk)

    def make_ikv(self, userinfo: dict, ikv: dict) -> str:
        if not userinfo.get('expires') or not isinstance(userinfo.get('expires'), int):
            userinfo['expires'] = self.expires
        userinfo['timestamp'] = int(time.time() * 1000)
        userinfo['__tokenId__'] = ikv.get('id')
        tk = self.aes_encrypt(json.dumps(userinfo), ikv['key'], ikv['iv'])
        return self._encode(tk)

    def make_token(self, info: dict, id_: str = None, key: str = None) -> str:
        riv = randstring(self.iv_length)
        tid = id_ or self.rand_id()
        token = self.make_ikv(info, {
            'id': tid,
            'iv': self.fix_iv(riv),
            'key': self.fix_key(key) if key else self.key,
        })
        return f"{riv}.{token}"

    def rand_iv_token(self, info: dict, id_: str = None, key: str = None) -> dict:
        riv = randstring(16)
        tid = id_ or self.rand_id()
        opts = {
            'id': tid,
            'key': self.fix_key(key) if key else self.key,
            'iv': self.fix_iv(riv),
        }
        opts['token'] = self.make_ikv(info, opts)
        return opts

    # ── Token 验证 ────────────────────────────────────────────────

    def verify(self, edata: str, ikv: dict = None) -> dict:
        if ikv is None:
            ikv = {}
        try:
            raw = self.aes_decrypt(edata, ikv.get('key') or self.key, ikv.get('iv') or self.iv)
            uj = json.loads(raw.decode('utf-8'))
            tm = int(time.time() * 1000)

            if uj['timestamp'] + uj['expires'] < tm:
                return {'ok': False, 'errcode': 'TIMEOUT'}

            token_id = uj.get('__tokenId__')
            ikv_id = ikv.get('id')
            if token_id and (
                (ikv_id and token_id != ikv_id) or token_id not in self.token_map
            ):
                return {'ok': False, 'errcode': 'ILLEGAL'}

            return {'ok': True, 'data': uj, 'now': tm}
        except Exception:
            return {'ok': False, 'errcode': 'FAILED'}

    def verify_token(self, edata: str) -> dict:
        parts = edata.split('.')
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return {'ok': False, 'errcode': 'ILLEGAL'}
        return self.verify(parts[1], {'iv': parts[0]})

    def verify_ikv(self, edata: str, ikv: dict) -> dict:
        return self.verify(edata, ikv)

    def verify_id(self, edata: str, tid: str) -> dict:
        tk = self.id_key_iv.get(tid)
        if tk:
            return self.verify(edata, tk)
        return self.verify(edata)

    def refresh_token(self, t: dict, ikv: dict = None) -> Optional[str]:
        data = t['data']
        if data['expires'] + data['timestamp'] - t['now'] < self.refresh:
            if ikv and isinstance(ikv, dict):
                return self.make_ikv(data, ikv)
            return self.make(data, data.get('__tokenId__'))
        return None


# ── 使用示例 ──────────────────────────────────────────────────────

if __name__ == '__main__':
    tt = WebxToken()
    tt.add_token_id(['user', 'admin'])

    print("AES backend:", AES_BACKEND)

    # 生成 token
    token = tt.make_token({'userId': 42, 'role': 'admin'}, id_='admin')
    print("Token:", token)

    # 验证 token
    result = tt.verify_token(token)
    print("Verify:", result)

    # refresh 演示
    tt.set_refresh(True)
    new_token = tt.refresh_token(result)
    print("Refreshed:", new_token)


# ── 全局实例（供 webx.middleware / core.guards 使用）────────────────

# 配置缺失时的兜底密钥。生产环境必须通过 TOKEN_SECRET 覆盖。
_FALLBACK_SECRET = 'default-secret-key-change-me-in-production'

_global_token: WebxToken | None = None


def get_token(
    key: str = None,
    expires_ms: int = None,
    token_ids: list[str] = None,
) -> WebxToken:
    """获取全局 WebxToken 实例（单例）。

    Args:
        key: 加密密钥，默认取 ``settings.TOKEN_SECRET``
        expires_ms: 有效期毫秒数，默认取 ``settings.TOKEN_EXPIRE_MINUTES``
        token_ids: 可签发/校验的 token id 集合，默认取 ``settings.TOKEN_IDS``

    使用示例::

        xt = get_token()                                    # 走配置
        xt = get_token(key='my-secret-key', expires_ms=3600000)   # 自定义
    """
    global _global_token

    if _global_token is None:
        # 配置不可用（如脱离应用的单元测试）时退到内置默认值，保证本模块可独立使用
        try:
            from webx.config import settings
            secret = key or settings.TOKEN_SECRET
            expires = expires_ms or (settings.TOKEN_EXPIRE_MINUTES * 60 * 1000)
            ids = token_ids or list(settings.TOKEN_IDS)
        except (ImportError, AttributeError):
            secret = key or _FALLBACK_SECRET
            expires = expires_ms or 10800000  # 3 小时
            ids = token_ids or ['user', 'admin']

        # fix_key 要求 key 长度 >= 16
        if len(secret) < 16:
            secret = _FALLBACK_SECRET

        _global_token = WebxToken({
            'key': secret,
            'expires': expires,
            'algorithm': 'aes-256-gcm',
        })
        _global_token.add_token_id(ids or ['user'])

    return _global_token


def reset_token() -> None:
    """清空单例（仅测试用：换密钥后需重建实例）。"""
    global _global_token
    _global_token = None
