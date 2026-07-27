"""正则表达式工具。"""
import re

# 邮箱：本地段允许 . - _（点不可在首尾/相邻），**刻意禁止 +**（杜绝 plus-addressing 别名绕过注册）；
# 域名支持多级（mail.example.co.uk）。长度由调用方字段/库（email 列 max_length=60）兜底。
EMAIL_RE = re.compile(
    r"^[a-z0-9_-]+(?:\.[a-z0-9_-]+)*@(?:[a-z0-9-]+\.)+[a-z]{2,}$",
    re.IGNORECASE,
)
NAME_RE = re.compile(r"^([\u4e00-\u9fff]|[a-z\(\)])([\u4e00-\u9fff]|[a-z0-9\(\)]){4,35}$", re.IGNORECASE)
CNAME_RE = re.compile(r"^[\u4e00-\u9fff]([\u4e00-\u9fff]|[a-z0-9\(\)]){1,25}$", re.IGNORECASE)
MOBILE_RE = re.compile(r"^(12|13|14|15|16|17|18|19)[0-9]{9}$")
ORGCODE_RE = re.compile(r"^[A-Z0-9]{18}$")