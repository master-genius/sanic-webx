"""通用工具函数。"""
import hashlib
import random
import string
import time
from datetime import datetime


_SALT_CHARS = string.ascii_lowercase + string.digits


def randstr(length: int = 8) -> str:
    return "".join(random.choices(_SALT_CHARS, k=length))


def randstr2() -> str:
    return format(random.getrandbits(52), 'x') + format(int(time.time() * 1000), 'x')


def sha1(s: str, encoding: str = 'hex') -> str:
    h = hashlib.sha1(s.encode()).digest()
    return h.hex() if encoding == 'hex' else h.decode('latin-1')


def md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def sha512(s: str) -> str:
    return hashlib.sha512(s.encode()).hexdigest()


def gen_username() -> str:
    tm = str(int(time.time() * 1000))
    n = random.randint(101, 999)
    return f"u{tm[-4:]}{n}"


def fmt_time(mode: str = 'long', sep: str = ':') -> str:
    now = datetime.now()
    date_str = now.strftime('%Y-%m-%d')
    if mode == 'short':
        return date_str
    hour_min = now.strftime(f'%H{sep}%M')
    if mode == 'middle':
        return f"{date_str}_{now.strftime('%H')}"
    return f"{date_str}_{hour_min}{sep}{now.strftime('%S')}"


def now_ms() -> int:
    """获取当前 Unix 时间戳（毫秒）"""
    return int(time.time() * 1000)


def model_to_dict(model, exclude: list[str] = None) -> dict:
    """将 Tortoise ORM Model 转为原始 dict"""
    exclude = exclude or []
    return {
        f: getattr(model, f)
        for f in model._meta.fields_map
        if f not in exclude
    }


async def delay(ms: int = 500) -> None:
    import asyncio
    await asyncio.sleep(ms / 1000)


def mask_email(email: str) -> str:
    """邮箱脱敏：本地名保留首尾各 1 字符、中间统一替换为 ***，域名完整保留。

    用于登录响应等对外展示：前端凭「非空」判定已绑定，又不泄露完整邮箱。
    空值 / 无 @ 的非法值原样返回（空 → 空串，便于前端判「未绑定」）。

    例：alice@example.com → a***e@example.com；ab@x.com → a***@x.com；a@x.com → ***@x.com
    """
    if not email or "@" not in email:
        return email or ""
    local, _, domain = email.partition("@")
    if len(local) <= 1:
        masked = "***"
    elif len(local) == 2:
        masked = local[0] + "***"
    else:
        masked = local[0] + "***" + local[-1]
    return f"{masked}@{domain}"


def mask_mobile(mobile: str) -> str:
    """手机号脱敏：保留前 3 后 4、中间统一 ***；过短则保留首尾各 1。

    空值返回空串（便于前端判「未绑定」）。例：13800138000 → 138***8000
    """
    if not mobile:
        return ""
    if len(mobile) >= 7:
        return f"{mobile[:3]}***{mobile[-4:]}"
    if len(mobile) <= 2:
        return "***"
    return f"{mobile[0]}***{mobile[-1]}"