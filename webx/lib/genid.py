import secrets
import threading
from datetime import datetime

def _init_generator():
    """
    内部初始化逻辑：预计算字符映射表，利用闭包持久化状态。
    """
    # 核心 Base36 字符集
    CHARS = "0123456789abcdefghijklmnopqrstuvwxyz"
    C_LEN = len(CHARS)
    
    # 预计算所有 2 位 36 进制组合 (36 * 36 = 1296 项)
    # 用空间换时间，避免在生成 ID 时进行复杂的数学运算
    MS_LOOP = [x + y for x in CHARS for y in CHARS]
    
    # 按照原 JS 逻辑切片
    S_LOOP = MS_LOOP[:100]       # 用于 分(0-59)、秒(0-59)
    Y_LOOP = MS_LOOP[36:500]     # 用于年份偏移 (支持 2026 年起的 464 年)
    
    # 随机偏移毫秒映射表，增加跨机器/跨进程的唯一性
    MS_SHIFTED = MS_LOOP[secrets.randbelow(100):]
    
    # 闭包内的持久化状态
    state = {
        "y": secrets.randbelow(C_LEN),
        "m": secrets.randbelow(C_LEN),
        "d": secrets.randbelow(C_LEN),
        "start_year": 2026,  # 设定为 2026 年开始
        "lock": threading.Lock()
    }

    def sid(id_len=18, id_pre=""):
        """
        生成 Serial ID。
        id_len: 默认 18，最小强制 16。
        id_pre: 可选前缀。
        """
        # 强制最小长度 16
        if id_len < 16:
            id_len = 16
            
        now = datetime.now()
        
        # 1. 生成 11 位时间轴部分 (Time-based part)
        y_idx = now.year - state["start_year"]
        if y_idx < 0 or y_idx >= len(Y_LOOP):
            y_idx = 0
            
        # 组装 11 位: 年(2) + 月(1) + 日(1) + 时(1) + 分(2) + 秒(2) + 毫秒(2)
        # 所有的索引操作都是 O(1) 性能极高
        time_part = (
            Y_LOOP[y_idx] +
            CHARS[now.month - 1] +
            CHARS[now.day] +
            CHARS[now.hour] +
            S_LOOP[now.minute] +
            S_LOOP[now.second] +
            MS_SHIFTED[now.microsecond // 1000]
        )

        # 2. 生成 3 位循环计数部分 (Counter-based part，线程安全)
        # 此部分保证了即使在同一毫秒内，单进程也能产生 46,656 个不重复 ID
        with state["lock"]:
            char_part = (
                CHARS[state["y"]] + 
                CHARS[state["m"]] + 
                CHARS[state["d"]]
            )
            # 内部状态累加
            state["d"] += 1
            if state["d"] >= C_LEN:
                state["d"] = 0
                state["m"] += 1
                if state["m"] >= C_LEN:
                    state["m"] = 0
                    state["y"] += 1
                    if state["y"] >= C_LEN:
                        state["y"] = 0

        # 基础核心 ID 长度为 14 位
        core_id = time_part + char_part
        
        # 3. 补齐随机位 (Random padding)
        # 默认 18 位情况下，会追加 4 位随机字符
        padding_len = id_len - 14
        padding = "".join(secrets.choice(CHARS) for _ in range(padding_len))
        
        return f"{id_pre}{core_id}{padding}"

    return sid

# --- 导出唯一函数接口 ---
# 模块加载时即完成闭包初始化
make_serial_id = _init_generator()

def gen_id():
    return make_serial_id(16)

# --- 使用演示 ---
if __name__ == "__main__":
    # 默认生成 18 位 ID
    # 结果示例: 2d33p1e131557h8asv (按时间轴升序)
    print(f"gen id (16): {gen_id()}")

    # 强制最小 16 位测试
    print(f"Minimum (16): {make_serial_id(16)}")

    # 带前缀的 20 位测试
    print(f"Prefix (20):  {make_serial_id(20, 'MD_')}")