"""演示任务脚本 —— 一个任务子进程能做的全部事情，不含任何业务。

worker 的 executor 以「任务目录」为 cwd 拉起本脚本，并追加 `--task-id <ID>`：

    <WORK_DIR>/<task_id>/
        params.json   任务参数（params_text 解析后的 JSON）
        files.json    输入文件清单（若有）        in/  输入文件已下载到这里
        meta.json     task_type / steps / sub_step / end_step / timeout_seconds …
        out/          约定的产物目录（本脚本把产物写在这里）

连接信息、fencing 令牌、存储地址都在环境变量里（executor 注入，见 webx_worker/envnames.py）：
本脚本通过 `webx_worker.sdk` 回调服务端，不需要自己拼 URL 和 header。

本脚本做五件事：读 params → 睡一会儿（模拟计算）→ 写产物 → 上报进度 → 交付产物。

params 可选字段：
    seconds   模拟计算时长（秒），缺省 1
    fail      为真则以非零码退出（演示失败路径）
"""

import json
import os
import sys
import time
from pathlib import Path

from webx_worker.sdk import result_client
from webx_worker.sdk.logsetup import configure_task_logging
from webx_worker.sdk.progress import report_step_message
from webx_worker.sdk.upload_route import deliver_only, deliver_required


def main() -> int:
    configure_task_logging()
    task_id = sys.argv[sys.argv.index("--task-id") + 1]
    params = json.loads(Path("params.json").read_text(encoding="utf-8"))
    meta = json.loads(Path("meta.json").read_text(encoding="utf-8"))
    sub_step = meta.get("sub_step", "")        # 单阶段任务恒为空串（空串是合法值，不是“没有”）

    print(f"task {task_id} type={meta['task_type']} params={params}")
    print(f"greeting from run_env: {os.environ.get('DEMO_GREETING')}")

    # 1. 上报进度（最佳努力：失败即弃，绝不为一条提示让计算干等）
    report_step_message(task_id, sub_step, "Preparing")

    # 2. 模拟计算
    time.sleep(float(params.get("seconds", 1)))
    if params.get("fail"):
        print("failing on purpose", file=sys.stderr)
        return 3

    # 3. 写产物
    out = Path("out")
    out.mkdir(exist_ok=True)
    (out / "result.txt").write_text(f"result of {task_id}\n", encoding="utf-8")
    (out / "payload.bin").write_bytes(b"\x00" * 4096)

    # 4. 交付产物 —— 两个入口，名字就是契约：
    #    deliver_required  下游依赖的产物：恒同步上传，多大都等（等的是正确性）
    #    deliver_only      纯交付给用户的产物：小的同步、超过阈值的交给 uploader 异步上传
    #                      （进队列时返回空串，uploader 传完会把真地址交付给任务）
    report_url = deliver_required(task_id, out / "result.txt", "result.txt", ["outputs", "report_url"])
    payload_url = deliver_only(task_id, out / "payload.bin", "payload.bin", ["outputs", "payload_url"])
    print(f"report_url={report_url!r} payload_url={payload_url!r}")

    # 5. 把已知地址写进任务的 result_path（merge：只覆盖叶子，不碰同级字段）
    if report_url:
        result_client.update_task_fields(
            task_id, result_path=json.dumps({"outputs": {"report_url": report_url}}), mode="merge"
        )
    report_step_message(task_id, sub_step, "Done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
