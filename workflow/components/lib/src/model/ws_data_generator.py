import os
import pandas as pd
import numpy as np
from datetime import timedelta
from pathlib import Path

def sliding_window_stats(csv_path: str, output_dir: str, window_sec: int):
    """
    对原始数据中的每个时间点，计算其回望窗口内的统计特征（均值、标准差）。
    窗口定义为 [t - window_sec, t]（闭区间），包含当前点。
    输出 CSV 的行数与原始数据相同，保留原始时间戳和标签，并新增 _avg、_std 列。

    :param csv_path: 输入 CSV 文件路径
    :param output_dir: 输出目录路径
    :param window_sec: 窗口大小（秒）
    """
    # 1. 读取数据
    df = pd.read_csv(csv_path)

    timestamp_col = "curTime_of_UTC8"
    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
    df = df.sort_values(timestamp_col).reset_index(drop=True)

    # 2. 确定数值列（排除时间戳列和 label 列）
    exclude_cols = {timestamp_col, "label"}
    numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns
                    if c not in exclude_cols]

    # 3. 为每一行计算窗口统计
    results = []
    for idx, row in df.iterrows():
        t = row[timestamp_col]
        window_start = t - timedelta(seconds=window_sec)
        # 窗口包含当前点，所以用 <= t
        mask = (df[timestamp_col] >= window_start) & (df[timestamp_col] <= t)
        window_data = df.loc[mask]

        out_row = {timestamp_col: t}

        # 数值列统计
        for col in numeric_cols:
            values = window_data[col]
            if len(values) > 0:
                out_row[f"{col}_avg"] = values.mean()
                # 标准差：若只有一个点，标准差为 0（总体标准差 ddof=0）
                out_row[f"{col}_std"] = values.std(ddof=0)
            else:
                # 理论上不会走到这里，因为当前点一定在窗口内
                out_row[f"{col}_avg"] = np.nan
                out_row[f"{col}_std"] = np.nan

        # 标签列：保留当前行的 label
        if "label" in df.columns:
            out_row["label"] = row["label"]

        results.append(out_row)

    # 4. 保存结果
    result_df = pd.DataFrame(results)
    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}_ws_{window_sec}.csv")
    result_df.to_csv(output_path, index=False)

    print(f"处理完成，结果已保存至：{output_path}，共计 {len(result_df)} 行（与原始数据行数相同）")

# 使用示例
if __name__ == "__main__":
    # 输入数据目录
    input_dir = r'D:\XFC_files\code\UDP2026\data\processed\input_model\video' 
    output_dir = r'D:\XFC_files\code\UDP2026\data\processed\input_nt_model'
    # 选择输出时窗大小
    window_sec = 10
    # 每个业务独立输出
    scene = Path(input_dir).name
    scene_output_dir = os.path.join(output_dir, scene)
    if not os.path.exists(scene_output_dir):
        os.makedirs(scene_output_dir)
    
    csvfiles = [file for file in os.listdir(input_dir) if file.endswith('.csv')]

    for csv in csvfiles:
        input_file = os.path.join(input_dir, csv)
        sliding_window_stats(input_file, scene_output_dir, window_sec)