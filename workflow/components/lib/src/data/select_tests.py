import os
import re
import argparse
import random
import secrets
import pandas as pd

def extract_basename(filename: str) -> str:
    """
    从文件名中提取基础名：按下划线分割，取前两部分（例如 gaming_2025102601），忽略后续内容。
    例如：
        "gaming_2025102601_combinedUDPModem.csv" -> "gaming_2025102601"
        "gaming_2025102601_combinedUDPModem_windowed.csv" -> "gaming_2025102601"
        "other_file.csv" -> "other_file" (fallback)
    """
    # 先去掉文件扩展名
    base = filename
    if filename.endswith(".csv"):
        base = filename[:-4]   # 去掉 ".csv"

    # 按下划线分割
    parts = base.split('_')
    if len(parts) >= 2:
        rest = "_" + "_".join(parts[2:])
        return (parts[0]+'_'+parts[1]), rest+'.csv'
    else:
        return base

def find_file_by_basename(directory: str, basename: str) -> str:
    """
    在目录中查找以 basename 开头的 .csv 文件，返回完整文件名，若找不到则返回 None。
    """
    if not os.path.isdir(directory):
        return None
    for f in os.listdir(directory):
        if f.lower().endswith('.csv') and f.startswith(basename):
            return f
    return None

def select_tests_from_dir(directory: str, label_column: str = "label", seed=None):
    """
    在指定目录中，按 label==1 行数排序并三等分，每档随机选一个文件，
    返回选中的文件的基础名列表（由 extract_basename 提取）。
    """
    if seed is not None:
        random.seed(seed)

    if not os.path.isdir(directory):
        print(f"目录不存在: {directory}")
        return []

    # 收集所有 CSV 文件
    csv_files = [f for f in os.listdir(directory) if f.lower().endswith(".csv")]
    if not csv_files:
        print(f"目录中未找到 CSV 文件: {directory}")
        return []

    results = []  # (basename, count_ones)
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
        basename, rest = extract_basename(filename)
        results.append((basename, int(count_ones)))

    if not results:
        print("没有可统计的文件")
        return []

    # 按 count_ones 从高到低排序
    results.sort(key=lambda x: x[1], reverse=True)

    n = len(results)
    k = max(1, n // 3)
    high = results[:k]
    mid = results[k:2*k]
    low = results[2*k:]

    selected_basenames = []
    for tier in [high, mid, low]:
        if tier:
            selected_basenames.append(random.choice(tier)[0])
    tests = []
    for basename in selected_basenames:
        tests.append(basename + rest)

    return tests

def count_label_ones_in_dir(directory: str, label_column: str = "label") -> None:
    """
    统计目录下每个 CSV 中 label==1 的行数，并打印。
    """
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
    input_dir1 = r'E:\UDP2026\data\processed\input_model\gaming'
    print(f'{select_tests_from_dir(input_dir1, seed=139395583)}')
    input_dir2 = r'E:\UDP2026\data\processed\input_nt_model\gaming'
    print(f'{select_tests_from_dir(input_dir2, seed=139395583)}')
    count_label_ones_in_dir(input_dir1)
    count_label_ones_in_dir(input_dir2)

if __name__ == "__main__":
    main()