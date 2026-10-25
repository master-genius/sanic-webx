"""Worker HTTP 文件下载客户端。

通过任务服务的 Internal API 下载任务的输入文件
（`GET /internal/tasks/<id>/files` 与 `.../files/<name>`，docs/worker-protocol.md §4.11）。
"""

import logging
from urllib.parse import quote

import httpx

from . import envnames as E
from .config import worker_settings
from .link_health import observed_async_client

logger = logging.getLogger(__name__)


class HttpFileClient:
    """HTTP 文件下载客户端"""

    def __init__(self):
        self._client: httpx.AsyncClient | None = None

    # 访问期现读 worker_settings，理由与边界同 callback.WebCallbackClient 的同名属性
    # （模块级单例在 import 期构造，早于 parse_args()；构造期拷贝会冻死启动参数覆盖）。

    @property
    def base_url(self) -> str:
        return worker_settings.WEB_BASE_URL

    @property
    def internal_token(self) -> str:
        """⚠️ 同 callback：会被烤进懒建的 httpx 默认 header，仅保证首次请求前的覆盖生效。"""
        return worker_settings.INTERNAL_TOKEN

    async def _get_client(self) -> httpx.AsyncClient:
        """获取或创建 httpx 客户端"""
        if self._client is None or self._client.is_closed:
            # 与 callback 同一条链路（同 WEB_BASE_URL / 同 token / 同 TLS），
            # 故共用同一个观测咽喉，见 link_health.observed_async_client。
            #
            # 同样挂 fencing 请求钩子：协议 §4 规定内部端点「除特别说明外都需要 fencing」，
            # 下载发生在持有租约期间，带上令牌无副作用；若服务端对读接口也比对租户，
            # 不带令牌就会下载失败。（钩子在无租约时什么也不加。）
            from .callback import _inject_fencing_headers

            self._client = observed_async_client(
                headers={
                    E.HEADER_INTERNAL_TOKEN: self.internal_token,
                },
                timeout=30.0,
                event_hooks={"request": [_inject_fencing_headers]},
            )
        return self._client

    async def close(self):
        """关闭客户端"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def download_file(self, task_id: str, filename: str, local_path: str) -> bool:
        """下载文件到本地。

        Args:
            task_id: 任务 ID
            filename: 文件名
            local_path: 本地保存路径

        Returns:
            是否下载成功
        """
        client = await self._get_client()
        # 文件名必须百分号编码（含空格/中文/斜杠的名字原样拼进路径会 404 或被误解析）
        url = f"{self.base_url}/internal/tasks/{task_id}/files/{quote(filename, safe='')}"

        try:
            async with client.stream("GET", url) as resp:
                if resp.status_code != 200:
                    logger.error(f"下载文件失败: {task_id}/{filename}, 状态码: {resp.status_code}")
                    return False

                with open(local_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=8192):
                        f.write(chunk)

                return True
        except Exception as e:
            logger.error(f"下载文件异常: {task_id}/{filename}, 错误: {e}")
            return False

    async def list_files(self, task_id: str) -> list[str]:
        """列出任务的所有文件。

        Args:
            task_id: 任务 ID

        Returns:
            文件名列表
        """
        client = await self._get_client()
        url = f"{self.base_url}/internal/tasks/{task_id}/files"

        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("files", [])
            else:
                logger.error(f"列出文件失败: {task_id}, 状态码: {resp.status_code}")
                return []
        except Exception as e:
            logger.error(f"列出文件异常: {task_id}, 错误: {e}")
            return []


# 全局单例
http_file_client = HttpFileClient()
