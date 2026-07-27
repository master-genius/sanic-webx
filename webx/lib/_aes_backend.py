"""
AES-GCM / AES-CBC 原语后端（供 xytoken / 支付 / 证书复用）。

设计：native 优先 + 纯 Python 兜底，二者算法格式 100% 一致（同一 AES）。
  - EVPBackend ：用 stdlib `ctypes` 绑定『CPython 本身已加载』的 OpenSSL libcrypto
                 的 EVP_* 接口（零 pip、零编译、零额外分发；libcrypto 是 CPython
                 为 hashlib/ssl 早已链接的传递依赖）。native + AES-NI，约 2000× 于纯 Python，
                 且 ctypes 调用期释放 GIL，不阻塞 asyncio 事件循环。
  - PureBackend：纯标准库手写 AES（从旧实现逐字搬运），作可移植兜底与回归基准。

选择策略（_select_backend）：import 期用固定向量让 EVP 与 Pure **双向对拍**
（EVP 密文须能被 Pure 解、反之亦然），全过才启用 EVP，任一失败静默降级 Pure + warning。

对外只暴露四个与旧实现同名同签名的函数 + BACKEND 标识：
  aes_gcm_encrypt(key, iv, plaintext, aad=b'') -> (ciphertext, tag)
  aes_gcm_decrypt(key, iv, ciphertext, tag, aad=b'') -> plaintext
  aes_cbc_encrypt(key, iv, plaintext) -> ciphertext            # PKCS7
  aes_cbc_decrypt(key, iv, ciphertext) -> plaintext            # PKCS7
"""

from __future__ import annotations

import ctypes
import ctypes.util
import logging
import secrets
from ctypes import POINTER, byref, c_char_p, c_int, c_void_p

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════
#  纯 Python AES（从旧实现 xytoken-old.py 逐字搬运，行为零偏移）
# ════════════════════════════════════════════════════════════════════════

# AES S-Box
SBOX = bytes([
    0x63,0x7c,0x77,0x7b,0xf2,0x6b,0x6f,0xc5,0x30,0x01,0x67,0x2b,0xfe,0xd7,0xab,0x76,
    0xca,0x82,0xc9,0x7d,0xfa,0x59,0x47,0xf0,0xad,0xd4,0xa2,0xaf,0x9c,0xa4,0x72,0xc0,
    0xb7,0xfd,0x93,0x26,0x36,0x3f,0xf7,0xcc,0x34,0xa5,0xe5,0xf1,0x71,0xd8,0x31,0x15,
    0x04,0xc7,0x23,0xc3,0x18,0x96,0x05,0x9a,0x07,0x12,0x80,0xe2,0xeb,0x27,0xb2,0x75,
    0x09,0x83,0x2c,0x1a,0x1b,0x6e,0x5a,0xa0,0x52,0x3b,0xd6,0xb3,0x29,0xe3,0x2f,0x84,
    0x53,0xd1,0x00,0xed,0x20,0xfc,0xb1,0x5b,0x6a,0xcb,0xbe,0x39,0x4a,0x4c,0x58,0xcf,
    0xd0,0xef,0xaa,0xfb,0x43,0x4d,0x33,0x85,0x45,0xf9,0x02,0x7f,0x50,0x3c,0x9f,0xa8,
    0x51,0xa3,0x40,0x8f,0x92,0x9d,0x38,0xf5,0xbc,0xb6,0xda,0x21,0x10,0xff,0xf3,0xd2,
    0xcd,0x0c,0x13,0xec,0x5f,0x97,0x44,0x17,0xc4,0xa7,0x7e,0x3d,0x64,0x5d,0x19,0x73,
    0x60,0x81,0x4f,0xdc,0x22,0x2a,0x90,0x88,0x46,0xee,0xb8,0x14,0xde,0x5e,0x0b,0xdb,
    0xe0,0x32,0x3a,0x0a,0x49,0x06,0x24,0x5c,0xc2,0xd3,0xac,0x62,0x91,0x95,0xe4,0x79,
    0xe7,0xc8,0x37,0x6d,0x8d,0xd5,0x4e,0xa9,0x6c,0x56,0xf4,0xea,0x65,0x7a,0xae,0x08,
    0xba,0x78,0x25,0x2e,0x1c,0xa6,0xb4,0xc6,0xe8,0xdd,0x74,0x1f,0x4b,0xbd,0x8b,0x8a,
    0x70,0x3e,0xb5,0x66,0x48,0x03,0xf6,0x0e,0x61,0x35,0x57,0xb9,0x86,0xc1,0x1d,0x9e,
    0xe1,0xf8,0x98,0x11,0x69,0xd9,0x8e,0x94,0x9b,0x1e,0x87,0xe9,0xce,0x55,0x28,0xdf,
    0x8c,0xa1,0x89,0x0d,0xbf,0xe6,0x42,0x68,0x41,0x99,0x2d,0x0f,0xb0,0x54,0xbb,0x16,
])

INV_SBOX = bytes([SBOX.index(i) for i in range(256)])

RCON = [0x00,0x01,0x02,0x04,0x08,0x10,0x20,0x40,0x80,0x1b,0x36]


def _rotword(word):
    return word[1:] + word[:1]


def _subword(word):
    return bytes(SBOX[b] for b in word)


def _gmul(a, b):
    p = 0
    for _ in range(8):
        if b & 1:
            p ^= a
        hi = a & 0x80
        a = (a << 1) & 0xff
        if hi:
            a ^= 0x1b
        b >>= 1
    return p


class _PureAES:
    """纯 Python AES 实现（支持 128/192/256 位密钥）。"""

    def __init__(self, key: bytes):
        key_len = len(key)
        if key_len == 16:
            self.nr, self.nk = 10, 4
        elif key_len == 24:
            self.nr, self.nk = 12, 6
        elif key_len == 32:
            self.nr, self.nk = 14, 8
        else:
            raise ValueError(f"Invalid AES key length: {key_len}")
        self.round_keys = self._key_expansion(key)

    def _key_expansion(self, key):
        w = [list(key[i*4:(i+1)*4]) for i in range(self.nk)]
        for i in range(self.nk, 4 * (self.nr + 1)):
            temp = w[i-1][:]
            if i % self.nk == 0:
                temp = list(_subword(_rotword(bytes(temp))))
                temp[0] ^= RCON[i // self.nk]
            elif self.nk > 6 and i % self.nk == 4:
                temp = list(_subword(bytes(temp)))
            w.append([w[i-self.nk][j] ^ temp[j] for j in range(4)])
        return [bytes(w[i] + w[i+1] + w[i+2] + w[i+3]) for i in range(0, len(w)-3, 4)]

    def _add_round_key(self, state, rk):
        return [[state[r][c] ^ rk[r + 4*c] for c in range(4)] for r in range(4)]

    def _sub_bytes(self, state):
        return [[SBOX[state[r][c]] for c in range(4)] for r in range(4)]

    def _inv_sub_bytes(self, state):
        return [[INV_SBOX[state[r][c]] for c in range(4)] for r in range(4)]

    def _shift_rows(self, state):
        return [
            [state[0][0], state[0][1], state[0][2], state[0][3]],
            [state[1][1], state[1][2], state[1][3], state[1][0]],
            [state[2][2], state[2][3], state[2][0], state[2][1]],
            [state[3][3], state[3][0], state[3][1], state[3][2]],
        ]

    def _inv_shift_rows(self, state):
        return [
            [state[0][0], state[0][1], state[0][2], state[0][3]],
            [state[1][3], state[1][0], state[1][1], state[1][2]],
            [state[2][2], state[2][3], state[2][0], state[2][1]],
            [state[3][1], state[3][2], state[3][3], state[3][0]],
        ]

    def _mix_columns(self, state):
        def mix(col):
            return [
                _gmul(col[0],2)^_gmul(col[1],3)^col[2]^col[3],
                col[0]^_gmul(col[1],2)^_gmul(col[2],3)^col[3],
                col[0]^col[1]^_gmul(col[2],2)^_gmul(col[3],3),
                _gmul(col[0],3)^col[1]^col[2]^_gmul(col[3],2),
            ]
        return [[mix([state[r][c] for r in range(4)])[r] for c in range(4)] for r in range(4)]

    def _inv_mix_columns(self, state):
        def imix(col):
            return [
                _gmul(col[0],0xe)^_gmul(col[1],0xb)^_gmul(col[2],0xd)^_gmul(col[3],0x9),
                _gmul(col[0],0x9)^_gmul(col[1],0xe)^_gmul(col[2],0xb)^_gmul(col[3],0xd),
                _gmul(col[0],0xd)^_gmul(col[1],0x9)^_gmul(col[2],0xe)^_gmul(col[3],0xb),
                _gmul(col[0],0xb)^_gmul(col[1],0xd)^_gmul(col[2],0x9)^_gmul(col[3],0xe),
            ]
        return [[imix([state[r][c] for r in range(4)])[r] for c in range(4)] for r in range(4)]

    def _bytes_to_state(self, block: bytes):
        return [[block[r + 4*c] for c in range(4)] for r in range(4)]

    def _state_to_bytes(self, state):
        return bytes(state[r][c] for c in range(4) for r in range(4))

    def encrypt_block(self, block: bytes) -> bytes:
        state = self._bytes_to_state(block)
        state = self._add_round_key(state, self.round_keys[0])
        for rd in range(1, self.nr):
            state = self._sub_bytes(state)
            state = self._shift_rows(state)
            state = self._mix_columns(state)
            state = self._add_round_key(state, self.round_keys[rd])
        state = self._sub_bytes(state)
        state = self._shift_rows(state)
        state = self._add_round_key(state, self.round_keys[self.nr])
        return self._state_to_bytes(state)

    def decrypt_block(self, block: bytes) -> bytes:
        state = self._bytes_to_state(block)
        state = self._add_round_key(state, self.round_keys[self.nr])
        for rd in range(self.nr - 1, 0, -1):
            state = self._inv_shift_rows(state)
            state = self._inv_sub_bytes(state)
            state = self._add_round_key(state, self.round_keys[rd])
            state = self._inv_mix_columns(state)
        state = self._inv_shift_rows(state)
        state = self._inv_sub_bytes(state)
        state = self._add_round_key(state, self.round_keys[0])
        return self._state_to_bytes(state)


def _xor_bytes(a: bytes, b: bytes) -> bytes:
    return bytes(x ^ y for x, y in zip(a, b))


def _pure_cbc_encrypt(key: bytes, iv: bytes, plaintext: bytes) -> bytes:
    """AES-CBC 加密（PKCS7 padding）。"""
    aes = _PureAES(key)
    pad_len = 16 - len(plaintext) % 16
    plaintext += bytes([pad_len] * pad_len)
    ciphertext = b''
    prev = iv
    for i in range(0, len(plaintext), 16):
        block = _xor_bytes(plaintext[i:i+16], prev)
        enc = aes.encrypt_block(block)
        ciphertext += enc
        prev = enc
    return ciphertext


def _pure_cbc_decrypt(key: bytes, iv: bytes, ciphertext: bytes) -> bytes:
    """AES-CBC 解密（PKCS7 unpadding）。"""
    aes = _PureAES(key)
    plaintext = b''
    prev = iv
    for i in range(0, len(ciphertext), 16):
        block = ciphertext[i:i+16]
        dec = aes.decrypt_block(block)
        plaintext += _xor_bytes(dec, prev)
        prev = block
    pad_len = plaintext[-1]
    return plaintext[:-pad_len]


def _ghash(h: bytes, data: bytes) -> bytes:
    """GHASH for GCM。"""
    def gmul128(x: int, y: int) -> int:
        R = 0xe1 << 120
        z = 0
        for i in range(128):
            if (y >> (127 - i)) & 1:
                z ^= x
            if x & 1:
                x = (x >> 1) ^ R
            else:
                x >>= 1
        return z

    h_int = int.from_bytes(h, 'big')
    result = 0
    if len(data) % 16:
        data += b'\x00' * (16 - len(data) % 16)
    for i in range(0, len(data), 16):
        block = int.from_bytes(data[i:i+16], 'big')
        result ^= block
        result = gmul128(result, h_int)
    return result.to_bytes(16, 'big')


def _pure_gcm_encrypt(key: bytes, iv: bytes, plaintext: bytes, aad: bytes = b'') -> tuple[bytes, bytes]:
    """AES-GCM 加密，返回 (ciphertext, tag)。"""
    aes = _PureAES(key)
    H = aes.encrypt_block(b'\x00' * 16)
    if len(iv) == 12:
        j0 = iv + b'\x00\x00\x00\x01'
    else:
        padded = iv
        if len(padded) % 16:
            padded += b'\x00' * (16 - len(padded) % 16)
        padded += b'\x00' * 8 + (len(iv) * 8).to_bytes(8, 'big')
        j0 = _ghash(H, padded)

    def gctr(icb, data):
        if not data:
            return b''
        out = b''
        cb = int.from_bytes(icb, 'big')
        for i in range(0, len(data), 16):
            enc = aes.encrypt_block(cb.to_bytes(16, 'big'))
            chunk = data[i:i+16]
            out += _xor_bytes(enc[:len(chunk)], chunk)
            cb = (cb & ~0xffffffff) | ((cb + 1) & 0xffffffff)
        return out

    cb_start = (int.from_bytes(j0, 'big') & ~0xffffffff) | ((int.from_bytes(j0, 'big') + 1) & 0xffffffff)
    ciphertext = gctr(cb_start.to_bytes(16, 'big'), plaintext)

    def pad16(b):
        return b + b'\x00' * ((16 - len(b) % 16) % 16)

    ghash_input = pad16(aad) + pad16(ciphertext) + \
                  (len(aad) * 8).to_bytes(8, 'big') + (len(ciphertext) * 8).to_bytes(8, 'big')
    S = _ghash(H, ghash_input)
    tag_full = gctr(j0, S)
    tag = tag_full[:16]
    return ciphertext, tag


def _pure_gcm_decrypt(key: bytes, iv: bytes, ciphertext: bytes, tag: bytes, aad: bytes = b'') -> bytes:
    """AES-GCM 解密，验证 tag 后返回明文。"""
    aes = _PureAES(key)
    H = aes.encrypt_block(b'\x00' * 16)
    if len(iv) == 12:
        j0 = iv + b'\x00\x00\x00\x01'
    else:
        padded = iv
        if len(padded) % 16:
            padded += b'\x00' * (16 - len(padded) % 16)
        padded += b'\x00' * 8 + (len(iv) * 8).to_bytes(8, 'big')
        j0 = _ghash(H, padded)

    def gctr(icb, data):
        if not data:
            return b''
        out = b''
        cb = int.from_bytes(icb, 'big')
        for i in range(0, len(data), 16):
            enc = aes.encrypt_block(cb.to_bytes(16, 'big'))
            chunk = data[i:i+16]
            out += _xor_bytes(enc[:len(chunk)], chunk)
            cb = (cb & ~0xffffffff) | ((cb + 1) & 0xffffffff)
        return out

    def pad16(b):
        return b + b'\x00' * ((16 - len(b) % 16) % 16)

    ghash_input = pad16(aad) + pad16(ciphertext) + \
                  (len(aad) * 8).to_bytes(8, 'big') + (len(ciphertext) * 8).to_bytes(8, 'big')
    S = _ghash(H, ghash_input)
    tag_full = gctr(j0, S)
    expected_tag = tag_full[:16]

    if not secrets.compare_digest(expected_tag, tag):
        raise ValueError("GCM authentication tag mismatch")

    cb_start = (int.from_bytes(j0, 'big') & ~0xffffffff) | ((int.from_bytes(j0, 'big') + 1) & 0xffffffff)
    return gctr(cb_start.to_bytes(16, 'big'), ciphertext)


# ════════════════════════════════════════════════════════════════════════
#  EVP 后端（ctypes 绑定『已在场』的 libcrypto；native + AES-NI）
# ════════════════════════════════════════════════════════════════════════

# EVP_CIPHER_CTX_ctrl 命令码（OpenSSL 1.1/3.0 稳定）
_EVP_CTRL_GCM_SET_IVLEN = 0x9
_EVP_CTRL_GCM_GET_TAG = 0x10
_EVP_CTRL_GCM_SET_TAG = 0x11
_GCM_TAG_LEN = 16


class _EVPCrypto:
    """绑定进程内 libcrypto 的 EVP AES-GCM / AES-CBC（按密钥长度自动选 128/192/256）。"""

    def __init__(self) -> None:
        self._C = self._load_libcrypto()
        self._bind_signatures()
        # 按 (mode, key_len) 缓存 EVP_CIPHER* 句柄（getter 返回的是全局静态指针，可缓存）
        self._cipher_cache: dict[tuple[str, int], int] = {}

    @staticmethod
    def _load_libcrypto() -> ctypes.CDLL:
        # 兼容 OpenSSL 3.0(.so.3) 与 1.1(.so.1.1)；优先用 find_library 解析的真实路径
        for name in ("crypto", "libcrypto.so.3", "libcrypto.so.1.1", "libcrypto.so"):
            resolved = ctypes.util.find_library(name) or name
            try:
                return ctypes.CDLL(resolved)
            except OSError:
                continue
        raise OSError("libcrypto not found")

    def _bind_signatures(self) -> None:
        # ★ 关键：必须显式声明 argtypes/restype，否则 ctypes 默认按 c_int 处理指针，
        #   64 位指针被截断 → 段错误（已实测踩坑）。
        C = self._C
        C.EVP_CIPHER_CTX_new.restype = c_void_p
        C.EVP_CIPHER_CTX_new.argtypes = []
        C.EVP_CIPHER_CTX_free.argtypes = [c_void_p]
        C.EVP_CIPHER_CTX_free.restype = None
        for getter in ("EVP_aes_128_gcm", "EVP_aes_192_gcm", "EVP_aes_256_gcm",
                       "EVP_aes_128_cbc", "EVP_aes_192_cbc", "EVP_aes_256_cbc"):
            fn = getattr(C, getter)
            fn.restype = c_void_p
            fn.argtypes = []
        C.EVP_EncryptInit_ex.argtypes = [c_void_p, c_void_p, c_void_p, c_char_p, c_char_p]
        C.EVP_EncryptInit_ex.restype = c_int
        C.EVP_DecryptInit_ex.argtypes = [c_void_p, c_void_p, c_void_p, c_char_p, c_char_p]
        C.EVP_DecryptInit_ex.restype = c_int
        C.EVP_EncryptUpdate.argtypes = [c_void_p, c_char_p, POINTER(c_int), c_char_p, c_int]
        C.EVP_EncryptUpdate.restype = c_int
        C.EVP_DecryptUpdate.argtypes = [c_void_p, c_char_p, POINTER(c_int), c_char_p, c_int]
        C.EVP_DecryptUpdate.restype = c_int
        C.EVP_EncryptFinal_ex.argtypes = [c_void_p, c_char_p, POINTER(c_int)]
        C.EVP_EncryptFinal_ex.restype = c_int
        C.EVP_DecryptFinal_ex.argtypes = [c_void_p, c_char_p, POINTER(c_int)]
        C.EVP_DecryptFinal_ex.restype = c_int
        C.EVP_CIPHER_CTX_ctrl.argtypes = [c_void_p, c_int, c_int, c_void_p]
        C.EVP_CIPHER_CTX_ctrl.restype = c_int

    def _cipher(self, mode: str, key_len: int) -> int:
        cached = self._cipher_cache.get((mode, key_len))
        if cached is not None:
            return cached
        bits = key_len * 8
        if bits not in (128, 192, 256):
            raise ValueError(f"Invalid AES key length: {key_len}")
        getter = getattr(self._C, f"EVP_aes_{bits}_{mode}")
        cipher = getter()
        if not cipher:
            raise RuntimeError(f"EVP_aes_{bits}_{mode} unavailable")
        self._cipher_cache[(mode, key_len)] = cipher
        return cipher

    # ── GCM ──────────────────────────────────────────────────────────
    def gcm_encrypt(self, key: bytes, iv: bytes, plaintext: bytes, aad: bytes = b'') -> tuple[bytes, bytes]:
        C = self._C
        ctx = C.EVP_CIPHER_CTX_new()
        if not ctx:
            raise RuntimeError("EVP_CIPHER_CTX_new failed")
        try:
            cipher = self._cipher("gcm", len(key))
            if C.EVP_EncryptInit_ex(ctx, cipher, None, None, None) != 1:
                raise RuntimeError("EncryptInit(cipher) failed")
            if C.EVP_CIPHER_CTX_ctrl(ctx, _EVP_CTRL_GCM_SET_IVLEN, len(iv), None) != 1:
                raise RuntimeError("set ivlen failed")
            if C.EVP_EncryptInit_ex(ctx, None, None, key, iv) != 1:
                raise RuntimeError("EncryptInit(key/iv) failed")
            outl = c_int()
            if aad:
                C.EVP_EncryptUpdate(ctx, None, byref(outl), aad, len(aad))
            buf = ctypes.create_string_buffer(len(plaintext) + 16)
            if C.EVP_EncryptUpdate(ctx, buf, byref(outl), plaintext, len(plaintext)) != 1:
                raise RuntimeError("EncryptUpdate failed")
            ciphertext = buf.raw[:outl.value]
            fin = c_int()
            if C.EVP_EncryptFinal_ex(ctx, buf, byref(fin)) != 1:
                raise RuntimeError("EncryptFinal failed")
            ciphertext += buf.raw[:fin.value]  # GCM 无填充，通常为空
            tag = ctypes.create_string_buffer(_GCM_TAG_LEN)
            if C.EVP_CIPHER_CTX_ctrl(ctx, _EVP_CTRL_GCM_GET_TAG, _GCM_TAG_LEN, tag) != 1:
                raise RuntimeError("get tag failed")
            return ciphertext, tag.raw[:_GCM_TAG_LEN]
        finally:
            C.EVP_CIPHER_CTX_free(ctx)

    def gcm_decrypt(self, key: bytes, iv: bytes, ciphertext: bytes, tag: bytes, aad: bytes = b'') -> bytes:
        C = self._C
        ctx = C.EVP_CIPHER_CTX_new()
        if not ctx:
            raise RuntimeError("EVP_CIPHER_CTX_new failed")
        try:
            cipher = self._cipher("gcm", len(key))
            if C.EVP_DecryptInit_ex(ctx, cipher, None, None, None) != 1:
                raise RuntimeError("DecryptInit(cipher) failed")
            if C.EVP_CIPHER_CTX_ctrl(ctx, _EVP_CTRL_GCM_SET_IVLEN, len(iv), None) != 1:
                raise RuntimeError("set ivlen failed")
            if C.EVP_DecryptInit_ex(ctx, None, None, key, iv) != 1:
                raise RuntimeError("DecryptInit(key/iv) failed")
            outl = c_int()
            if aad:
                C.EVP_DecryptUpdate(ctx, None, byref(outl), aad, len(aad))
            buf = ctypes.create_string_buffer(len(ciphertext) + 16)
            if C.EVP_DecryptUpdate(ctx, buf, byref(outl), ciphertext, len(ciphertext)) != 1:
                raise RuntimeError("DecryptUpdate failed")
            plaintext = buf.raw[:outl.value]
            # 写回期望 tag → Final 时校验
            tag_buf = ctypes.create_string_buffer(bytes(tag), len(tag))
            if C.EVP_CIPHER_CTX_ctrl(ctx, _EVP_CTRL_GCM_SET_TAG, len(tag), tag_buf) != 1:
                raise RuntimeError("set tag failed")
            fin = c_int()
            if C.EVP_DecryptFinal_ex(ctx, buf, byref(fin)) != 1:
                # tag 校验失败 → 与纯实现抛同类异常，语义一致
                raise ValueError("GCM authentication tag mismatch")
            plaintext += buf.raw[:fin.value]
            return plaintext
        finally:
            C.EVP_CIPHER_CTX_free(ctx)

    # ── CBC（OpenSSL 默认开启 PKCS7 填充，与纯实现一致）─────────────────
    def cbc_encrypt(self, key: bytes, iv: bytes, plaintext: bytes) -> bytes:
        C = self._C
        ctx = C.EVP_CIPHER_CTX_new()
        if not ctx:
            raise RuntimeError("EVP_CIPHER_CTX_new failed")
        try:
            cipher = self._cipher("cbc", len(key))
            if C.EVP_EncryptInit_ex(ctx, cipher, None, key, iv) != 1:
                raise RuntimeError("EncryptInit failed")
            outl = c_int()
            buf = ctypes.create_string_buffer(len(plaintext) + 16)
            if C.EVP_EncryptUpdate(ctx, buf, byref(outl), plaintext, len(plaintext)) != 1:
                raise RuntimeError("EncryptUpdate failed")
            ciphertext = buf.raw[:outl.value]
            fin = c_int()
            if C.EVP_EncryptFinal_ex(ctx, buf, byref(fin)) != 1:
                raise RuntimeError("EncryptFinal failed")
            ciphertext += buf.raw[:fin.value]
            return ciphertext
        finally:
            C.EVP_CIPHER_CTX_free(ctx)

    def cbc_decrypt(self, key: bytes, iv: bytes, ciphertext: bytes) -> bytes:
        C = self._C
        ctx = C.EVP_CIPHER_CTX_new()
        if not ctx:
            raise RuntimeError("EVP_CIPHER_CTX_new failed")
        try:
            cipher = self._cipher("cbc", len(key))
            if C.EVP_DecryptInit_ex(ctx, cipher, None, key, iv) != 1:
                raise RuntimeError("DecryptInit failed")
            outl = c_int()
            buf = ctypes.create_string_buffer(len(ciphertext) + 16)
            if C.EVP_DecryptUpdate(ctx, buf, byref(outl), ciphertext, len(ciphertext)) != 1:
                raise RuntimeError("DecryptUpdate failed")
            plaintext = buf.raw[:outl.value]
            fin = c_int()
            if C.EVP_DecryptFinal_ex(ctx, buf, byref(fin)) != 1:
                raise ValueError("CBC decrypt/padding failed")
            plaintext += buf.raw[:fin.value]
            return plaintext
        finally:
            C.EVP_CIPHER_CTX_free(ctx)


# ════════════════════════════════════════════════════════════════════════
#  后端选择：import 期 EVP↔Pure 双向对拍，全过才启用 EVP
# ════════════════════════════════════════════════════════════════════════

def _selftest_against_pure(evp: _EVPCrypto) -> None:
    """用固定向量让 EVP 与 Pure 互相加解密，确保两者格式 100% 一致；任一不符即抛错降级。"""
    pt = b'webx-aes-backend-selftest::userId=42::role=admin::0123456789'
    for key_len, iv_len in ((32, 12), (24, 12), (16, 12)):
        key = bytes(range(key_len))
        iv = bytes(range(iv_len))
        # GCM：EVP 加密 → Pure 解密 + Pure 加密 → EVP 解密（双向）
        c1, t1 = evp.gcm_encrypt(key, iv, pt)
        assert _pure_gcm_decrypt(key, iv, c1, t1) == pt, "GCM EVP->Pure"
        c2, t2 = _pure_gcm_encrypt(key, iv, pt)
        assert evp.gcm_decrypt(key, iv, c2, t2) == pt, "GCM Pure->EVP"
        assert (c1, t1) == (c2, t2), "GCM 密文/标签逐字节须相等"
        # tag 篡改必须被拒
        try:
            evp.gcm_decrypt(key, iv, c1, b'\x00' * 16)
            raise AssertionError("GCM 篡改未被拒")
        except ValueError:
            pass
    # CBC：16 字节 IV，双向对拍
    for key_len in (32, 24, 16):
        key = bytes(range(key_len))
        iv = bytes(range(16))
        e1 = evp.cbc_encrypt(key, iv, pt)
        e2 = _pure_cbc_encrypt(key, iv, pt)
        assert e1 == e2, "CBC 密文逐字节须相等"
        assert _pure_cbc_decrypt(key, iv, e1) == pt, "CBC EVP->Pure"
        assert evp.cbc_decrypt(key, iv, e2) == pt, "CBC Pure->EVP"


def _select_backend():
    """返回 (backend_name, gcm_enc, gcm_dec, cbc_enc, cbc_dec)。"""
    try:
        evp = _EVPCrypto()
        _selftest_against_pure(evp)
        logger.info("xytoken AES 后端: EVP (libcrypto %s, native+AES-NI)", evp._C._name)
        return ("evp", evp.gcm_encrypt, evp.gcm_decrypt, evp.cbc_encrypt, evp.cbc_decrypt)
    except Exception as exc:  # noqa: BLE001 - 任何异常都安全降级到纯实现
        logger.warning("EVP 后端不可用，降级为纯 Python AES（性能下降）: %s", exc)
        return ("pure", _pure_gcm_encrypt, _pure_gcm_decrypt, _pure_cbc_encrypt, _pure_cbc_decrypt)


(
    BACKEND,
    aes_gcm_encrypt,
    aes_gcm_decrypt,
    aes_cbc_encrypt,
    aes_cbc_decrypt,
) = _select_backend()


# 始终导出纯实现别名，供回归对拍 / 显式调用（不受后端选择影响）
pure_aes_gcm_encrypt = _pure_gcm_encrypt
pure_aes_gcm_decrypt = _pure_gcm_decrypt
pure_aes_cbc_encrypt = _pure_cbc_encrypt
pure_aes_cbc_decrypt = _pure_cbc_decrypt
