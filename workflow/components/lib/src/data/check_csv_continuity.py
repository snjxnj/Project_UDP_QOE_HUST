import argparse
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def parse_args():
    parser = argparse.ArgumentParser(description="CSV数据文件缺失值检测工具")
    parser.add_argument('--data-dir', type=str, default='.', help='包含待检测CSV文件的目录路径')
    parser.add_argument('--threshold', type=float, default=3.0, help='连续性阈值（秒），相邻两行间隔大于该值视为不连续')
    return parser.parse_args()

def scan_csv_files(data_dir: str) -> List[Path]:
    """递归扫描并返回根目录下所有 .csv/.CSV 文件的路径列表（按字典序排序）。"""
    root = Path(data_dir).resolve() # 规范化为绝对路径
    if not root.exists():
        raise FileNotFoundError(f"目录不存在: {root}")
    files = sorted(root.rglob("*.[cC][sS][vV]")) # Path.rglob递归查找所有 .csv/.CSV 文件
    return files

def check_csv_continuity(file_path: Path, ts_threshold) -> List[Tuple[int, int]]:
    try:
        csv_df = pd.read_csv(file_path)
    except Exception as e:
        print(f"读取失败: {file_path} | {e}")
        return []

    if 'curTime_of_UTC8' not in csv_df.columns:
        print(f"跳过（无curTime_of_UTC8列）: {file_path}")
        return []

    # 按固定格式解析完整日期时间，失败的行设为NaT并过滤
    raw_ts = csv_df['curTime_of_UTC8'].astype(str).str.strip()

    # 先尝试：YYYY-MM-DD HH:MM:SS.fff
    dt = pd.to_datetime(raw_ts, format="%Y-%m-%d %H:%M:%S.%f", errors="coerce")
    mask = dt.isna()

    # 第二轮：YYYY-MM-DD HH:MM:SS
    if mask.any():
        dt2 = pd.to_datetime(raw_ts[mask], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        dt[mask] = dt2
        mask = dt.isna()

    # 第三轮：HH:MM:SS.fff（无日期，前面拼一个固定日期）
    if mask.any():
        ts_with_date = "1970-01-01 " + raw_ts[mask]
        dt2 = pd.to_datetime(ts_with_date, format="%Y-%m-%d %H:%M:%S.%f", errors="coerce")
        dt[mask] = dt2
        mask = dt.isna()

    # 第四轮：HH:MM:SS（无日期无毫秒）
    if mask.any():
        ts_with_date = "1970-01-01 " + raw_ts[mask]
        dt2 = pd.to_datetime(ts_with_date, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        dt[mask] = dt2

    valid = dt.notna()
    if valid.sum() < 2:
        print(f"跳过（有效时间戳不足）: {file_path}")
        return []

    raw_ts = raw_ts[valid].reset_index(drop=True)
    dt = dt[valid].reset_index(drop=True)

    diffs = dt.diff().dt.total_seconds().to_numpy()
    anomalies: List[Tuple[int, int]] = []
    for i in range(1, len(diffs)):
        gap = diffs[i]
        if pd.isna(gap):
            continue
        if gap > float(ts_threshold):  # 大于阈值判为不连续
            print(f"异常文件: {file_path.name} | 开始时间戳: {raw_ts.iloc[i-1]} | 间隔(秒): {gap:.6f}")
            anomalies.append((i - 1, i))
    return anomalies

def check_all_csv_files_continuity(csv_files: List[Path], ts_threshold):   
    any_anomaly = False

    for csv_file in csv_files:
        res = check_csv_continuity(csv_file, ts_threshold=ts_threshold)
        any_anomaly = any_anomaly or bool(res)

    if not any_anomaly:
        print("未发现时间不连续的 CSV 文件。")

def get_all_features_var(csv_files: List[Path]):
    """整合所有CSV为一个DataFrame，排除 timestamp/label，对每列（数值特征）求方差并打印。"""
    dfs = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            # 排除不参与计算的列
            df = df.drop(columns=["timestamp", "label"], errors="ignore")
            dfs.append(df)
        except Exception as e:
            print(f"读取失败: {csv_file} | {e}")
            continue

    if not dfs:
        print("未读取到任何CSV文件。")
        return

    big_df = pd.concat(dfs, ignore_index=True, sort=False)

    # 将各列尽量转换为数值，非数值转为NaN
    big_num = big_df.apply(pd.to_numeric, errors='coerce')

    # 计算样本方差（pandas默认 ddof=1），跳过NaN
    var_series = big_num.var(skipna=True)

    # 去除全NaN列并按降序输出
    var_series = var_series.dropna().sort_values(ascending=False)

    print(f"整合后总行数: {len(big_df)}, 总列数: {big_df.shape[1]}")
    print("各列方差（降序，仅数值列，排除 timestamp/label）：")
    for col, var in var_series.items():
        print(f"{col}: {var:.6f}")


def main():
    args = parse_args()
    csv_files = scan_csv_files(args.data_dir)

    # 检查所有原始数据文件的时间不连续点
    check_all_csv_files_continuity(csv_files, args.threshold)

    get_all_features_var(csv_files)
    
if __name__ == "__main__":
    main()