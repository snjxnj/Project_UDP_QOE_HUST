import os
import re
import glob
from datetime import datetime
from typing import List, Iterable, Tuple

"""文件相关操作"""
def find_lag_time_list_files(root_dir: str) -> list[str]:
    """
    返回给定根目录下所有以 _lag_timeList.txt 结尾的文件（递归搜索）。
    若目录不存在或参数为空，返回空列表。
    """
    if not root_dir:
        return []
    if not os.path.isdir(root_dir):
        return []
    pattern = os.path.join(root_dir, '**', '*_lag_timeList.txt')
    files = glob.glob(pattern, recursive=True)
    # 只保留以 .txt 结尾的路径，返回排序后的列表
    return sorted([f for f in files if f.lower().endswith('.txt')])

def get_data_label_dir():
    """获取与当前脚本同级的 data_label 目录"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(script_dir)
    return os.path.join(root_dir, "data_label")

def ensure_result_dir() -> str:
    """返回脚本同级 result 目录路径，如不存在则创建"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    result_dir = os.path.join(script_dir, 'result')
    os.makedirs(result_dir, exist_ok=True)
    return result_dir

def ensure_result_pic_dir() -> str:
    """返回脚本同级 result_pic 可视化文件目录路径，如不存在则创建"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    result_dir = os.path.join(script_dir, 'result_pic')
    os.makedirs(result_dir, exist_ok=True)
    return result_dir

def ensure_filter_result_dir() -> str:
    """返回脚本同级 filter_result 按时间过滤后的文件目录路径，如不存在则创建"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    result_dir = os.path.join(script_dir, 'filter_result')
    os.makedirs(result_dir, exist_ok=True)
    return result_dir

def save_analysis_text(result_dir: str, label_file: str, total_lag_seconds: float, lag_intervals: List[Tuple[str, float]]):
    """将单个 label 的分析结果保存为同名 txt 到 result 目录"""
    base_name = os.path.basename(label_file)  # 保持同名对应
    out_path = os.path.join(result_dir, base_name)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(f"源标签文件: {label_file}\n")
        f.write(f"卡顿次数: {len(lag_intervals)} 次\n")
        f.write(f"总卡顿时间: {total_lag_seconds:.3f} 秒 ({format_time(total_lag_seconds)})\n")
        if lag_intervals:
            f.write("\n卡顿区间详情:\n")
            for i, (interval, duration) in enumerate(lag_intervals, 1):
                f.write(f"  {i:2d}. {interval} -> {duration:.3f}秒\n")
    return out_path

def label_filter_by_lag_time(result_dir: str, label_file: str, filter_time_limit: float, lag_intervals: List[Tuple[str, float]]):
    """根据卡顿时间区间过滤标签文件，保存到 filter_result 目录"""
    base_name = os.path.basename(label_file)  # 保持同名对应
    out_path = os.path.join(result_dir, base_name)
    with open(out_path, 'w', encoding='utf-8') as f:
        for (interval, duration) in lag_intervals:
            if duration >= filter_time_limit:
                f.write(f"{interval}\n")

# 处理note.txt脚本
def build_note_dict_from_file(note_path: str | None = None) -> dict:
    """
    读取 note.txt，返回字典：{ note: [(ID前10位, scene), ...] }
    若未提供 note_path，则按脚本目录/上级/data_label 查找第一个存在的 note.txt。
    忽略空行和以 # 开头的注释行；若无法解析 ID 则跳过该行。
    """
    candidate_paths = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidate_paths = [
        os.path.join(script_dir, "note.txt"),
        os.path.join(os.path.dirname(script_dir), "note.txt"),
        os.path.join(os.path.dirname(script_dir), "data_label", "note.txt"),
    ]
    if note_path:
        candidate_paths.insert(0, note_path)

    note_file = None
    for p in candidate_paths:
        if p and os.path.exists(p):
            note_file = p
            break

    if not note_file:
        return {}

    note_dict: dict[str, list[tuple[str, str]]] = {}
    # 用于去重（可选）
    seen_per_note: dict[str, set[tuple[str, str]]] = {}

    try:
        with open(note_file, 'r', encoding='utf-8') as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith('#'):
                    continue

                # 提取 ID（优先按 ID: 或 ID=，兜底匹配 10+ 位数字）
                m_id = re.search(r"ID\s*[:=]\s*(\d+)", line, re.I)
                if not m_id:
                    m_id = re.search(r"(\d{10,})", line)
                if not m_id:
                    continue
                id10 = m_id.group(1)[:10]

                # 提取 scene 与 note
                m_scene = re.search(r"scene\s*[:=]\s*([^,;]+)", line, re.I)
                scene = m_scene.group(1).strip() if m_scene else ""

                m_note = re.search(r"note\s*[:=]\s*([^,;]+)", line, re.I)
                note = m_note.group(1).strip() if m_note else "unknown"

                tup = (id10, scene)
                if note not in seen_per_note:
                    seen_per_note[note] = set()
                    note_dict[note] = []
                if tup not in seen_per_note[note]:
                    seen_per_note[note].add(tup)
                    note_dict[note].append(tup)
    except Exception:
        return {}

    return note_dict

"""时间戳处理相关操作"""
def time_to_seconds(time_str):
    """将时间字符串转换为秒数"""
    # 兼容 00:00:00.000 或 00:00:00 格式
    time_parts = time_str.split(':')
    hours = int(time_parts[0])
    minutes = int(time_parts[1])
    seconds_millis = time_parts[2].split('.')
    seconds = int(seconds_millis[0])
    milliseconds = int(seconds_millis[1]) if len(seconds_millis) > 1 else 0
    
    total_seconds = hours * 3600 + minutes * 60 + seconds + milliseconds / 1000.0
    return total_seconds

def calculate_lag_time(file_path):
    """计算单个文件的卡顿总时间"""
    total_lag_seconds = 0.0
    lag_intervals = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line or '-' not in line:
                    continue
                
                # 分割开始和结束时间
                start_time, end_time = line.split('-')
                start_seconds = time_to_seconds(start_time.strip())
                end_seconds = time_to_seconds(end_time.strip())
                # 如果结束时间小于开始时间，视为跨午夜，尾部加 24h
                if end_seconds < start_seconds:
                    end_seconds += 24 * 3600
                lag_duration = max(0.0, end_seconds - start_seconds)
                total_lag_seconds += lag_duration
                lag_intervals.append((line, lag_duration))
                
    except Exception as e:
        print(f"读取文件 {file_path} 时出错: {e}")
        return 0.0, []
    
    return total_lag_seconds, lag_intervals

def format_time(seconds):
    """格式化时间为易读格式"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = seconds % 60
    milliseconds = int((secs - int(secs)) * 1000)
    
    if hours > 0:
        return f"{hours}小时{minutes}分钟{int(secs)}秒{milliseconds}毫秒"
    elif minutes > 0:
        return f"{minutes}分钟{int(secs)}秒{milliseconds}毫秒"
    else:
        return f"{int(secs)}秒{milliseconds}毫秒"

"""可视化相关操作"""
"""
在柱状图上按样本区间填充背景色

参数:
- ax: Axes 对象
- sample_range: 样本区间，例如 (0, 4) 表示从第1个到第5个等5个样本，是一个闭区间
- color: 背景色，例如 "green"
"""
def highlight_diff_cases(ax, sample_range, color, text = None):
    start, end = sample_range
    # 注意：柱状图的横坐标从0开始，每个柱子宽度为1，所以需要调整边界
    ax.axvspan(start - 0.5, end + 0.5, facecolor=color, alpha=0.3)
    text_y_position=0.95

    # 如果有文字内容，在背景区域上方居中显示
    if text:
        # 计算背景区域的中心位置
        x_center = (start + end) / 2
        
        # 获取y轴范围，用于计算文字位置
        ymin, ymax = ax.get_ylim()
        text_y = ymin + (ymax - ymin) * text_y_position
        
        # 添加文字
        ax.text(x_center, text_y, text, 
                ha='center', va='center',
                fontsize=10, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8))

"""
整图分区间处理背景
在这里，你只需要复制highlight_diff_cases(ax, (0,0), 'green', '宿舍')
参数：
- ax: Axes 对象 不用管
- parm2：（,)给一个区间，例如 (0, 4) 表示从第1个到第5个等5个样本，是一个双闭区间
- parm3：颜色字符串，例如 'green'
- parm4：文字内容，例如 '宿舍'等备注信息
"""
def pic_highlight_diff_cases(ax, note_intervals):
    """
    根据传入的 note_intervals 列表绘制背景色区域。
    note_intervals: [(start_idx, end_idx, note_str), ...]
    已知 note 使用固定颜色，其他 note 按备用色循环分配。
    """
    if not note_intervals:
        return

    # 已知 note -> 颜色映射，可根据需要扩充
    color_map = {
        '宿舍': '#2ecc71',            # 绿
        '教室': '#8b4513',            # 棕
        '商场': '#3498db',            # 蓝
        '地铁-移动SIM': '#e67e22',    # 橙
        '地铁-广电SIM': '#95a5a6',    # 灰
        '断网': '#b22222',            # 深红
        '断网-广电SIM': '#c0392b',    # 红
        '高铁-广电SIM': '#7f8c8d',    # 深灰
        '高铁-移动SIM': '#2980b9',    # 深蓝
        'unknown': '#ffffff',         # 白（或留空背景）
    }
    # 备用颜色池（当 note 不在 color_map 时使用），颜色尽量互不相近
    fallback_colors = [
        '#d4f1c5', '#f7d6bd', '#cfe8ff', '#ffe7a8', '#e6e6fa', '#ffd6e7',
        '#f2d0d9', '#d0f2f1', '#fbe4b7', '#e6f7d7', '#dcd0ff', '#ffdfd0',
        '#b3e5fc', '#ffcc80', '#c8e6c9', '#f8bbd0', '#b2ebf2', '#d1c4e9',
        '#ffccbc', '#c5cae9', '#a3e4d7', '#ffd3b6', '#e0bbe4', '#c6d8ff'
    ]
    for i, entry in enumerate(note_intervals):
        # 支持两种格式： (start,end,note) 或 (start,end)
        if len(entry) >= 3:
            start, end, note = entry[0], entry[1], entry[2]
        else:
            start, end = entry[0], entry[1]
            note = 'unknown'
        color = color_map.get(note, fallback_colors[i % len(fallback_colors)])
        highlight_diff_cases(ax, (start, end), color, note)

def plot_bars(result_dir: str, titles: Tuple[str, str], x_labels: List[str], counts: List[int], totals: List[float], note_dic) -> Tuple[str | None, str | None]:
    """绘制两幅柱状图：卡顿次数与总卡顿时间"""
    x_labels_tuples = []
    for item in x_labels:
        parts = item.split('_')
        x_labels_tuples.append((parts[0], parts[3]))
    note_list = []

    # 根据 note_dic 构造逆向字典并按 note 分组（组内保持原顺序），同步重排 counts 与 totals
    # note_dic 中的 tuples 为 (id10, scene)
    reverse_map: dict[tuple[str, str], str] = {}
    for note, tuples in (note_dic or {}).items():
        for id10, scene in tuples:
            reverse_map[(scene, id10)] = note

    for item in x_labels_tuples:
        if item in reverse_map:
            note = reverse_map[item]
            note_list.append(note)

    # 按 note 分组（保留组内原始顺序），初始化分组时保证存在键
    ordered_notes: List[str] = []
    grouped_labels: dict[str, List[str]] = {}
    grouped_counts: dict[str, List[int]] = {}
    grouped_totals: dict[str, List[float]] = {}

    for lbl, c, t, n in zip(x_labels, counts, totals, note_list):
        if n not in grouped_labels:
            grouped_labels[n] = []
            grouped_counts[n] = []
            grouped_totals[n] = []
            ordered_notes.append(n)
        grouped_labels[n].append(lbl)
        grouped_counts[n].append(c)
        grouped_totals[n].append(t)

    # --- 将 preferred_order 重排放在这里（在扁平化前），确保生效 ---
    preferred_order = ['宿舍', '教室', '商场', '断网', '地铁-移动SIM', '地铁-广电SIM', '高铁-移动SIM', '高铁-广电SIM']
    seen = set()
    new_ordered = []
    for p in preferred_order:
        if p in ordered_notes:
            new_ordered.append(p)
            seen.add(p)
    for n in ordered_notes:
        if n not in seen:
            new_ordered.append(n)
    ordered_notes = new_ordered
    # --- 重排完成，继续扁平化 ---
    new_x: List[str] = []
    new_counts: List[int] = []
    new_totals: List[float] = []
    for n in ordered_notes:
        new_x.extend(grouped_labels.get(n, []))
        new_counts.extend(grouped_counts.get(n, []))
        new_totals.extend(grouped_totals.get(n, []))

    x_labels, counts, totals = new_x, new_counts, new_totals

    # 生成 note 区间列表：[(start, end, note), ...] 便于后续调用 highlight_diff_cases
    note_intervals: List[tuple[int, int, str]] = []
    start_idx = 0
    for n in ordered_notes:
        length = len(grouped_labels.get(n, []))
        if length > 0:
            end_idx = start_idx + length - 1
            note_intervals.append((start_idx, end_idx, n))
            start_idx += length
        else:
            # 若某 note 意外为空，跳过
            continue

    # 在 grouped_labels, grouped_counts, grouped_totals 和 ordered_notes 已构造完成后，添加以下强制顺序逻辑：
    preferred_order = ['断网', '宿舍', '教室', '商场',  '地铁-移动SIM', '地铁-广电SIM', '高铁-移动SIM', '高铁-广电SIM']
    # 保留 preferred_order 中存在的 note（按 preferred_order 顺序），其余 note 按原先出现顺序追加
    seen = set()
    new_ordered = []
    for p in preferred_order:
        if p in ordered_notes:
            new_ordered.append(p)
            seen.add(p)
    for n in ordered_notes:
        if n not in seen:
            new_ordered.append(n)
    ordered_notes = new_ordered

    # 延迟导入 matplotlib，并使用无头后端；若缺失则跳过绘图
    try:
        import matplotlib  # type: ignore
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as e:
        print(f"警告：matplotlib 不可用，跳过绘图。原因：{e}")
        return None, None
    plt.rcParams['font.sans-serif'] = ['SimHei']
    # 图1：卡顿次数
    plt.figure(figsize=(max(8, len(x_labels) * 0.6), 5))
    bars = plt.bar(range(len(x_labels)), counts, color='#5B8FF9')

    ax1 = plt.gca()
    pic_highlight_diff_cases(ax1, note_intervals)

    plt.title(titles[0])
    plt.xlabel('样本')
    plt.ylabel('卡顿次数 (次)')
    plt.xticks(range(len(x_labels)), x_labels, rotation=45, ha='right')
    # 在柱顶显示数值
    for b, v in zip(bars, counts):
        plt.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{v}", ha='center', va='bottom', fontsize=9)
    plt.tight_layout()
    out1 = os.path.join(result_dir, 'lag_counts.png')
    plt.savefig(out1, dpi=150)
    plt.close()

    # 图2：总卡顿时间（秒）
    plt.figure(figsize=(max(8, len(x_labels) * 0.6), 5))
    bars = plt.bar(range(len(x_labels)), totals, color='#61DDAA')

    ax2 = plt.gca()
    pic_highlight_diff_cases(ax2, note_intervals)

    plt.title(titles[1])
    plt.xlabel('样本')
    plt.ylabel('总卡顿时间 (秒)')
    plt.xticks(range(len(x_labels)), x_labels, rotation=45, ha='right')
    for b, v in zip(bars, totals):
        plt.text(b.get_x() + b.get_width() / 2, b.get_height(), f"{v:.1f}", ha='center', va='bottom', fontsize=9)
    plt.tight_layout()
    out2 = os.path.join(result_dir, 'lag_total_seconds.png')
    plt.savefig(out2, dpi=150)
    plt.close()
    return out1, out2

"""样本排序相关操作"""
# 按默认时间戳初步排序
def give_back_dir_sorted_files(dir_name):
    # 在当前dir_name目录查找所有以 _lag_timeList.txt 结尾的文件
    file_pattern = os.path.join(dir_name, '**', '*_lag_timeList.txt')
    txt_files = glob.glob(file_pattern, recursive=True)

    if not os.path.isdir(dir_name):
        print(f"未找到目录: {dir_name}")
    else: print(f"搜索到目录: {dir_name}")

    if not txt_files:
        print(f"未找到匹配 {file_pattern} 的文件")
        return
    
    # 按样本名中的数字字符串排序（例如 2025102101 -> 2025-10-21 第 01 个样本）
    def _sample_sort_key(path: str):
        name = os.path.splitext(os.path.basename(path))[0]
        # 优先匹配 10 位（YYYYMMDDNN）
        m10 = re.findall(r"(\d{10})", name)
        if m10:
            s = m10[-1]  # 取最后一个匹配，防止前缀数字干扰
            date_str, idx_str = s[:8], s[8:]
            try:
                dt = datetime.strptime(date_str, "%Y%m%d")
                idx = int(idx_str)
                return (dt, idx, name)
            except Exception:
                pass
        # 次选：匹配 8 位日期 + 任意非数字 + 两位序号
        m = re.search(r"(\d{8}).*?(\d{2})", name)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y%m%d")
                idx = int(m.group(2))
                return (dt, idx, name)
            except Exception:
                pass
        # 兜底：使用极小时间 + 名称，保持稳定性
        return (datetime.min, 0, name)

    txt_files = sorted(txt_files, key=_sample_sort_key)

    return txt_files

# 手动重排
def sort_files_by_myself(txt_files: list[str], target_filename: str, position: int) -> list[str]:
    """
    手动重排：若 txt_files 中存在 basename == target_filename 的元素
    则移除并插入到指定位置（0 为首）。越界位置自动截到 [0, len]
    不存在时原样返回
    """
    if not isinstance(txt_files, list) or not target_filename:
        return txt_files
    # 归一化位置
    if position < 0:
        position = 0
    if position > len(txt_files):
        position = len(txt_files)
    # 寻找目标（按文件名精确匹配）
    match_index = None
    for i, p in enumerate(txt_files):
        if os.path.basename(p) == target_filename:
            match_index = i
            break
    if match_index is None:
        print(f"在手动重排范围中未找到目标文件{target_filename}")
        return txt_files  # 不存在
    # 取出并插入
    path_obj = txt_files.pop(match_index)
    txt_files.insert(position, path_obj)
    return txt_files

# 计算实验总时长相关操作
def _parse_hms(hms: str) -> int:
    # 形如 "HH-MM-SS"
    m = re.match(r'^\s*(\d{2})-(\d{2})-(\d{2})\s*$', hms)
    if not m:
        raise ValueError(f"非法时间格式: {hms}")
    h, mnt, s = map(int, m.groups())
    return h * 3600 + mnt * 60 + s

def sum_experiment_duration(txt_path: str) -> float:
    """
    读取 address_List.txt，逐行解析 start_time / end_time，求和总秒数并打印。
    - 支持跨午夜：若 end < start，则 end += 24h
    - 忽略无法解析的行（打印警告）
    返回总秒数（float）
    """
    if not os.path.isfile(txt_path):
        print(f"文件不存在: {txt_path}")
        return 0.0

    total_seconds = 0.0
    line_no = 0
    with open(txt_path, 'r', encoding='utf-8') as f:
        for raw in f:
            line_no += 1
            line = raw.strip()
            if not line:
                continue
            # 提取 start_time 和 end_time 字段值
            m_start = re.search(r'start_time\s*:\s*([0-9]{2}-[0-9]{2}-[0-9]{2})', line)
            m_end   = re.search(r'end_time\s*:\s*([0-9]{2}-[0-9]{2}-[0-9]{2})', line)
            if not m_start or not m_end:
                print(f"[第{line_no}行] 跳过：未找到 start_time/end_time")
                continue
            try:
                s_start = _parse_hms(m_start.group(1))
                s_end   = _parse_hms(m_end.group(1))
                if s_end < s_start:
                    s_end += 24 * 3600
                dur = max(0.0, s_end - s_start)
                total_seconds += dur
            except Exception as e:
                print(f"[第{line_no}行] 解析错误：{e}")
                continue

    # 打印总时长
    def _fmt(sec: float) -> str:
        sec_int = int(sec)
        h = sec_int // 3600
        m = (sec_int % 3600) // 60
        s = sec_int % 60
        return f"{h}小时{m}分钟{s}秒"

    print(f"总实验时长: {total_seconds:.3f} 秒 ({_fmt(total_seconds)})")
    return total_seconds

def main():
    # search_dir = get_data_label_dir()
    search_dir = r'D:\XFC_files\code\UDP2026\data_label'
    note_file_path = r'D:\XFC_files\code\UDP2026\tests\Tools\label_log_analyzer\note2.txt'
    txt_files = give_back_dir_sorted_files(search_dir)

    result_dir = ensure_result_dir()
    print(f"结果输出目录: {result_dir}")
    
    visual_dir = ensure_result_pic_dir()
    print(f"可视化图片输出目录: {visual_dir}")

    filter_dir = ensure_filter_result_dir()
    print(f"按卡顿区间时间过滤后结果输出目录: {filter_dir}")

    note_dic = build_note_dict_from_file(note_file_path)
    # 取出所有要绘制的样本对应的 (scene, id10) 元组集合
    note_tuples_set = set()
    for note, tuples in note_dic.items():
        for id10, scene in tuples:
            note_tuples_set.add((scene, id10))

    # 新增：构造逆向映射 (scene,id10) -> note，便于统计
    reverse_map = {}
    for note, tuples in (note_dic or {}).items():
        for id10, scene in tuples:
            reverse_map[(scene, id10)] = note

    # 新增：每个 note 的统计 { note: {"count": 累计次数, "time": 累计秒} }
    per_note_stats = {}

    # 汇总数据用于可视化
    x_labels: List[str] = []
    counts: List[int] = []
    totals: List[float] = []
    all_files_lag_seconds = 0.0

    for idx, file_path in enumerate(txt_files):
        total_lag_seconds, lag_intervals = calculate_lag_time(file_path)
        all_files_lag_seconds += total_lag_seconds

        # 保存单文件分析文本到 result/ 同名 txt
        out_txt = save_analysis_text(result_dir, file_path, total_lag_seconds, lag_intervals)
        
        label_filter_by_lag_time(filter_dir, file_path, 2, lag_intervals)
        # 样本名（去扩展名、_lag_timeList固定格式）
        sample_name = os.path.splitext(os.path.basename(file_path))[0]
        parts = sample_name.split('_')
        if (parts[0], parts[3][:10]) in note_tuples_set:
            if sample_name.endswith('_lag_timeList'):
                sample_name = sample_name[:-len('_lag_timeList')]

            x_labels.append(sample_name)
            counts.append(len(lag_intervals))
            totals.append(total_lag_seconds)

            # 新增：按 note 累计
            note_key = reverse_map.get((parts[0], parts[3][:10]), 'unknown')
            st = per_note_stats.setdefault(note_key, {"count": 0, "time": 0.0})
            st["count"] += len(lag_intervals)
            st["time"] += total_lag_seconds

    print('参与绘图统计的样本文件：')
    print('-' * 70)
    for xl in x_labels:
        print(xl)

    # 新增：按优先顺序打印每个 note 的统计
    preferred_order = ['断网', '宿舍', '教室', '商场', '地铁-移动SIM', '地铁-广电SIM', '高铁-移动SIM', '高铁-广电SIM']
    print('-' * 70)
    print('各 note 总统计：')
    for note in preferred_order:
        if note in per_note_stats:
            st = per_note_stats[note]
            print(f'{note}: lag_count={st["count"]} 次, lag_time={st["time"]:.3f} 秒 ({format_time(st["time"])})')
    # 打印未在优先列表中的其他 note
    for note, st in per_note_stats.items():
        if note not in preferred_order:
            print(f'{note}: lag_count={st["count"]} 次, lag_time={st["time"]:.3f} 秒 ({format_time(st["time"])})')

    # 生成两幅柱状图
    out1, out2 = plot_bars(
        visual_dir,
        ("各样本卡顿次数", "各样本总卡顿时间(秒)"),
        x_labels,
        counts,
        totals,
        note_dic,
    )
    if out1 and out2:
        print(f"柱状图已保存: \n{out1}\n{out2}")
    
    #exp_all_time = sum_experiment_duration(r'D:\XFC_files\code\UDP_QoE\data_PreProcessing\address_List.txt')
    print(f"检测到所有的{len(txt_files)}个样本文件，总卡顿时间: {all_files_lag_seconds:.3f} 秒 ({format_time(all_files_lag_seconds)})")
    #print(f"总卡顿时长{exp_all_time}s({format_time(exp_all_time)})，总卡顿时间占比: {all_files_lag_seconds / exp_all_time * 100:.4f}%")
    
    print('-' * 70)
    

# 先按“note”标志分组 然后组内按时间排序
if __name__ == "__main__":
    main()