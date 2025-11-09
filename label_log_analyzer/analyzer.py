import os
import re
import glob
from datetime import datetime
from typing import List, Iterable, Tuple

"""文件相关操作"""
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
def pic_highlight_diff_cases(ax):
    highlight_diff_cases(ax, (0,0), 'green', '宿舍')
    highlight_diff_cases(ax, (1,1), 'red', '断网')
    highlight_diff_cases(ax, (2,3), 'green', '宿舍')
    highlight_diff_cases(ax, (4,6), 'brown', '教室')
    highlight_diff_cases(ax, (7,9), 'blue', '商场')
    highlight_diff_cases(ax, (10,15), 'orange', '地铁-移动SIM')
    highlight_diff_cases(ax, (16,20), 'white', '地铁-广电SIM')
    highlight_diff_cases(ax, (21,30), 'red', '断网-广电SIM')

def plot_bars(result_dir: str, titles: Tuple[str, str], x_labels: List[str], counts: List[int], totals: List[float]):
    """绘制两幅柱状图：卡顿次数与总卡顿时间"""
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
    pic_highlight_diff_cases(ax1)

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
    pic_highlight_diff_cases(ax2)

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

def main():
    search_dir = get_data_label_dir()

    # 在\data_label目录查找所有以 _lag_timeList.txt 结尾的文件
    file_pattern = os.path.join(search_dir, '**', '*_lag_timeList.txt')
    print(file_pattern)
    txt_files = glob.glob(file_pattern, recursive=True)
    if not os.path.isdir(search_dir):
        print(f"未找到目录: {search_dir}")
    else: print(f"搜索到目录: {search_dir}")

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

    result_dir = ensure_result_dir()
    print(f"结果输出目录: {result_dir}")

    visual_dir = ensure_result_pic_dir()
    print(f"可视化图片输出目录: {visual_dir}")

    # 汇总数据用于可视化
    x_labels: List[str] = []
    counts: List[int] = []
    totals: List[float] = []

    for file_path in txt_files:
        total_lag_seconds, lag_intervals = calculate_lag_time(file_path)

        # 保存单文件分析文本到 result/ 同名 txt
        out_txt = save_analysis_text(result_dir, file_path, total_lag_seconds, lag_intervals)
        
        print(f"\n分析文件: {file_path}")
        print("-" * 40)
        
        print(f"分析结果已保存: {out_txt}")

        # 控制台简单摘要
        
        print(f"卡顿次数: {len(lag_intervals)} 次")
        print(f"总卡顿时间: {total_lag_seconds:.3f} 秒 ({format_time(total_lag_seconds)})")
        print("-" * 40)
        
        # 收集用于绘图的数据
        x_labels.append(os.path.splitext(os.path.basename(file_path))[0])
        counts.append(len(lag_intervals))
        totals.append(total_lag_seconds)

    # 生成两幅柱状图
    out1, out2 = plot_bars(
        visual_dir,
        ("各样本卡顿次数", "各样本总卡顿时间(秒)"),
        x_labels,
        counts,
        totals,
    )
    if out1 and out2:
        print(f"柱状图已保存: {out1}\n{out2}")

if __name__ == "__main__":
    main()