"""密码哈希模块。

基于标准库 hashlib.scrypt 实现，零第三方依赖。
存储格式：salt.hash（点连接）

scrypt 特性：
  - 迭代次数可调（n=2^k，默认 16384）
  - 内存密集型（块大小 r=8，默认约 16MB）
  - 专门抵抗 GPU/ASIC 暴力破解
"""

import base64
import hashlib
import hmac
import os


_SALT_LENGTH = 16  # 16 字节 = 128 bits 熵


class HashPassword:
    """密码哈希工具类。

    存储格式：``<salt>.<hash>``，其中 salt 和 hash 均为 Base64 编码。
    """

    def make(self, passwd: str) -> str:
        """生成密码哈希，返回 salt.hash 格式字符串。

        Args:
            passwd: 明文密码

        Returns:
            可直接存入数据库的哈希字符串
        """
        salt_bytes = os.urandom(_SALT_LENGTH)
        hash_bytes = hashlib.scrypt(
            passwd.encode("utf-8"),
            salt=salt_bytes,
            n=16384,
            r=8,
            p=1,
            dklen=64,  # 输出 512 位
        )

        salt_b64 = base64.b64encode(salt_bytes).decode("ascii").rstrip("=")
        hash_b64 = base64.b64encode(hash_bytes).decode("ascii").rstrip("=")

        return f"{salt_b64}.{hash_b64}"

    def verify(self, passwd: str, hash_passwd: str) -> dict:
        """验证明文密码与存储哈希是否匹配。

        原理：从 hash_passwd 中提取 salt → 用相同参数对 passwd 重算 → 比对结果

        Args:
            passwd:      明文密码
            hash_passwd: 数据库中存储的哈希字符串（``salt.hash`` 格式）

        Returns:
            ``{"ok": True}`` 或 ``{"ok": False, "errcode": "<原因>"}``

            errcode 说明：
            - ``"ERROR"``：格式非法（不含恰好一个 ``.``）
            - ``"WRONG"``：密码不匹配
            - ``"FAILED"``：验证过程异常
        """
        parts = hash_passwd.split(".")
        if len(parts) != 2:
            return {"ok": False, "errcode": "ERROR"}

        salt_b64, stored_hash = parts

        try:
            # Base64 解码（补回可能缺失的 "=" 填充）
            salt_pad = 4 - len(salt_b64) % 4
            if salt_pad != 4:
                salt_b64_padded = salt_b64 + "=" * salt_pad
            else:
                salt_b64_padded = salt_b64

            salt = base64.b64decode(salt_b64_padded)

            # 用相同的盐与参数重算哈希
            hash_bytes = hashlib.scrypt(
                passwd.encode("utf-8"),
                salt=salt,
                n=16384,
                r=8,
                p=1,
                dklen=64,
            )
            computed = base64.b64encode(hash_bytes).decode("ascii").rstrip("=")

            # 使用 hmac 防止时序攻击
            if hmac.compare_digest(computed, stored_hash):
                return {"ok": True}
            return {"ok": False, "errcode": "WRONG"}
        except Exception:
            return {"ok": False, "errcode": "FAILED"}
