import os
import random
import pandas as pd

def extract_basename(filename: str) -> tuple:
    """
    返回 (basename_prefix, rest_of_filename)
    例如: "gaming_2025102601_combinedUDPModem.csv" -> ("gaming_2025102601", "_combinedUDPModem.csv")
    """
    base = filename[:-4] if filename.endswith(".csv") else filename
    parts = base.split('_')
    if len(parts) >= 2:
        prefix = parts[0] + '_' + parts[1]
        rest = "_" + "_".join(parts[2:]) + ".csv"
        return prefix, rest
    else:
        return base, ".csv"

def find_file_by_basename(directory: str, basename: str) -> str:
    """根据基础名查找实际文件名（不推荐用于本次抽取，因为 rest 部分需保留）"""
    if not os.path.isdir(directory):
        return None
    for f in os.listdir(directory):
        if f.lower().endswith('.csv') and f.startswith(basename):
            return f
    return None

def select_test_val_from_dir(directory: str, label_column: str = "label", seed=None):
    """
    从目录中按 label==1 数量排序并分三档（高、中、低），每档各抽取两个文件：
    - 一个作为测试集
    - 一个作为验证集（不重叠）
    返回两个列表：test_files (完整文件名), val_files (完整文件名)
    """
    if seed is not None:
        random.seed(seed)

    if not os.path.isdir(directory):
        print(f"目录不存在: {directory}")
        return [], []

    csv_files = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
    if not csv_files:
        print(f"目录中未找到 CSV 文件: {directory}")
        return [], []

    # 统计每个文件的 label==1 数量
    file_stats = []  # (filename, count_ones)
    for filename in sorted(csv_files):
        filepath = os.path.join(directory, filename)
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            print(f"读取失败，跳过文件: {filename}，错误: {e}")
            continue
        if label_column not in df.columns:
            print(f"文件中不存在列 '{label_column}'，跳过: {filename}")
            continue
        count_ones = (df[label_column] == 1).sum()
        file_stats.append((filename, int(count_ones)))

    if not file_stats:
        print("没有可统计的文件")
        return [], []

    # 按 count_ones 从高到低排序
    file_stats.sort(key=lambda x: x[1], reverse=True)

    n = len(file_stats)
    # 分成三档
    k = max(1, n // 3)
    high = file_stats[:k]
    mid  = file_stats[k:2*k]
    low  = file_stats[2*k:]

    test_files = []
    val_files = []

    for tier in [high, mid, low]:
        if len(tier) == 0:
            continue
        elif len(tier) == 1:
            # 只有一个文件，只能作为测试或验证，这里优先给测试集
            test_files.append(tier[0][0])
            print(f"警告：档位文件数不足，仅将 {tier[0][0]} 放入测试集，验证集空缺")
        else:
            # 随机打乱该档，前两个分别作为测试和验证
            shuffled = random.sample(tier, len(tier))
            test_files.append(shuffled[0][0])
            val_files.append(shuffled[1][0])

    return test_files, val_files

def count_label_ones_in_dir(directory: str, label_column: str = "label") -> None:
    """统计并打印每个文件的 label==1 数量"""
    if not os.path.isdir(directory):
        print(f"目录不存在: {directory}")
        return
    csv_files = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
    if not csv_files:
        print(f"目录中未找到 CSV 文件: {directory}")
        return
    results = []
    for filename in sorted(csv_files):
        filepath = os.path.join(directory, filename)
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            print(f"读取失败，跳过文件: {filename}，错误: {e}")
            continue
        if label_column not in df.columns:
            print(f"文件中不存在列 '{label_column}'，跳过: {filename}")
            continue
        count_ones = (df[label_column] == 1).sum()
        results.append((filename, int(count_ones)))
    results.sort(key=lambda x: x[1], reverse=True)
    print("按 label==1 行数从高到低排序:")
    for filename, cnt in results:
        print(f"{filename}: {cnt}")

def main():
    input_dir1 = r'D:\XFC_files\code\UDP2026\data\processed\input_model\gaming'
    input_dir2 = r'D:\XFC_files\code\UDP2026\data\processed\input_nt_model\gaming'
    count_label_ones_in_dir(input_dir1)
    seed = 139395583
    print(f"使用种子: {seed}")
    """
    for d in [input_dir1, input_dir2]:
        print(f"\n目录: {d}")
        test_list, val_list = select_test_val_from_dir(d, seed=seed)
        print(f"测试集文件: {test_list}")
        print(f"验证集文件: {val_list}")
        print("\n原始统计:")
        count_label_ones_in_dir(d)"""

if __name__ == "__main__":
    main()