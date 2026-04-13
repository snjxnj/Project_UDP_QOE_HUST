import os
import glob
import pandas as pd
import numpy as np
import src.add_list.Timelabel as timer
import src.add_list.Listparser as listparser
#################################################
"""
该脚本主要负责从已经过滤过的modem CSV文件中，进一步处理所需的modem特征数据
原始特征列为字符串形式，未拆分5项取值，需要对单一特征但有多个取值的情况进行进一步处理

2025.12.23 现计算脚本对modem的全特征处理
2026.1.26 现版本融入了打标签功能
"""
#################################################

script_dir = os.path.dirname(os.path.abspath(__file__))

# 补充缺省值
inf = 99

EXCLUDE_FEATURES = {"mn_rsrp_4", "mn_snr_4"}
TIMESTAMP_COL = "timestamp"

#################################################
# 各种数据统计函数
def compute_mean(values):
    """均值"""
    if not values:
        return np.nan
    return float(np.mean(values))

def compute_std(values):
    """标准差（总体，ddof=0）"""
    if not values:
        return np.nan
    if len(values) == 1:
        return 0.0
    return float(np.std(values, ddof=0))

def compute_min(values):
    """最小值"""
    if not values:
        return np.nan
    return float(np.min(values))

def compute_max(values):
    """最大值"""
    if not values:
        return np.nan
    return float(np.max(values))

def compute_median(values):
    """中位数"""
    if not values:
        return np.nan
    return float(np.median(values))

STAT_FUNCS = {
    "mean": compute_mean,
    "std": compute_std,
    "min": compute_min,
    "max": compute_max,
    "median": compute_median,
}
#################################################
# 处理 mn_rsrp_4, mn_snr_4 等矩阵特征
def _parse_three_samples_four_antennas(cell: str):
    """
    解析形如：
      "{a1,a2,a3,a4},{b1,b2,b3,b4},{c1,c2,c3,c4}"
    返回数组形状为 (3, 4) 的浮点列表；无效值(-inf/inf/nan)过滤为 np.nan。
    若解析失败，返回空列表。
    """
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s:
        return []
    # 提取每个花括号里的内容
    groups = []
    buf = ""
    depth = 0
    for ch in s:
        if ch == '{':
            depth += 1
            if depth == 1:
                buf = ""
            else:
                buf += ch
        elif ch == '}':
            depth -= 1
            if depth == 0:
                groups.append(buf)
            else:
                buf += ch
        else:
            if depth >= 1:
                buf += ch
    if len(groups) == 0:
        return []
    samples = []
    for g in groups:
        tokens = [t.strip() for t in g.split(',') if t.strip() != ""]
        vals = []
        for t in tokens:
            tl = t.lower()
            if tl == "inf" or tl == "-inf":
                vals.append(inf)          # 使用占位值 99
                continue
            if tl == "nan":
                vals.append(np.nan)
                continue
            try:
                vals.append(float(t))
            except ValueError:
                vals.append(np.nan)
        samples.append(vals)
    # 期望每个样本4个值，若长度不一致，做齐到4（填充 NaN 或截断）
    samples_fixed = []
    for row in samples[:3]:  # 只取前3个采样
        r = list(row[:4])
        if len(r) < 4:
            r = r + [np.nan] * (4 - len(r))
        samples_fixed.append(r)
    # 保证为 (3,4)
    while len(samples_fixed) < 3:
        samples_fixed.append([np.nan, np.nan, np.nan, np.nan])
    return samples_fixed

def _aggregate_per_antenna(samples_3x4, op="mean"):
    """
    对 3×4 的矩阵按天线列聚合，返回长度为4的数组。
    op 支持：mean, min, max, median, std
    NaN 会被忽略（全部NaN则结果为 NaN）。
    """
    if not samples_3x4:
        return [np.nan]*4
    arr = np.array(samples_3x4, dtype=float)  # shape: (3,4)
    cols = []
    for i in range(arr.shape[1]):
        col = arr[:, i]
        col = col[~np.isnan(col)]
        if col.size == 0:
            cols.append(np.nan)
        else:
            if op == "mean":
                cols.append(float(np.mean(col)))
            elif op == "min":
                cols.append(float(np.min(col)))
            elif op == "max":
                cols.append(float(np.max(col)))
            elif op == "median":
                cols.append(float(np.median(col)))
            elif op == "std":
                cols.append(float(np.std(col, ddof=0)))
            else:
                cols.append(float(np.mean(col)))
    return cols

def add_antenna_features(row, op="mean"):
    """
    从当前行的 mn_rsrp_4、mn_snr_4 计算 rsrp_1..4, snr_1..4（按天线维度聚合3个采样）。
    返回字典 { "rsrp_1":..., "rsrp_2":..., "rsrp_3":..., "rsrp_4":..., "snr_1":..., ... }。
    """
    out = {}
    rsrp_samples = _parse_three_samples_four_antennas(row.get("mn_rsrp_4", ""))
    snr_samples = _parse_three_samples_four_antennas(row.get("mn_snr_4", ""))
    rsrp_agg = _aggregate_per_antenna(rsrp_samples, op=op)
    snr_agg = _aggregate_per_antenna(snr_samples, op=op)
    for i, v in enumerate(rsrp_agg, start=1):
        out[f"rsrp_{i}_{op}"] = v
    for i, v in enumerate(snr_agg, start=1):
        out[f"snr_{i}_{op}"] = v
    return out
#################################################
# 对单一特征但有多个取值的处理方法
def parse_values(cell: str):
    """
    将逗号分隔的取值解析为 float 列表。
    遇到 inf / -inf 用占位值 99；nan 跳过。
    """
    if pd.isna(cell):
        return []
    s = str(cell).strip()
    if not s:
        return []
    
    s = s.replace('{', '').replace('}', '')
    vals = []
    for token in s.split(','):
        token = token.strip()
        if not token:
            continue
        low = token.lower()
        if low == "inf" or low == "-inf":
            vals.append(inf)          # 使用占位值
            continue
        if low == "nan":
            continue
        try:
            v = float(token)
        except ValueError:
            continue
        vals.append(v)
    return vals

def process_one_filtered_csv(csv_path: str, output_dir: str, cmd: str, addlist:str):
    """
    读取一个已过滤的CSV，生成带统计量的DataFrame。
    返回 DataFrame（包含 source, timestamp 以及各统计列）。
    """
    parser = listparser.SampleListParser(addlist) if addlist is not None else None

    df = pd.read_csv(csv_path, encoding="utf-8")
    # 参与统计的列：去除 timestamp 与排除列
    feature_cols = [c for c in df.columns if c != TIMESTAMP_COL and c not in EXCLUDE_FEATURES]

    os.makedirs(output_dir, exist_ok=True)

    # suffix参与输出文件的命名
    cmd_lower = (cmd or "all").lower()  # 命令转小写
    if cmd_lower == "all":
        stats_to_apply = STAT_FUNCS
        suffix = "stats"
    else:
        if cmd_lower not in STAT_FUNCS:
            raise ValueError(f"不支持的 cmd: {cmd}. 允许: {', '.join(STAT_FUNCS.keys())} 或 'all'")
        stats_to_apply = {cmd_lower: STAT_FUNCS[cmd_lower]}
        suffix = cmd_lower

    rows = []
    for _, row in df.iterrows():
        out = {
            TIMESTAMP_COL: row.get(TIMESTAMP_COL, None)
        }
        for col in feature_cols:
            vals = parse_values(row.get(col, ""))
            if col in ['rf_sul_band']:
                out[col] = compute_mean(vals)
                continue

            if(len(vals) == 1):
                # 只有一个值时，保留原值
                out[col] = vals[0]
            else:
                for stat_name, func in stats_to_apply.items():
                    out[f"{col}_{stat_name}"] = func(vals)
        # 加入8列（按天线聚合3采样 对mn_rsrp_4/mn_snr_4的处理）
        ant_feats = add_antenna_features(row, op = cmd_lower)
        out.update(ant_feats)
        rows.append(out)
    out_df = pd.DataFrame(rows)

    base = os.path.basename(csv_path)
    parts = base.split('_')
    sample_key = parts[0] + '_' + parts[1]
    if parser is not None:
        out_df = timer.label_dataframe_by_intervals(out_df,parser.get_sample_info(sample_key)['lag_timeList_path'])
    else:
        print("警告：未提供 addlist，可能未生成标签！")

    out_name = base.replace("_filtered_modem_data.csv", f"_{suffix}.csv")
    if out_name == base:
        name, ext = os.path.splitext(base)
        out_name = f"{name}_{suffix}{ext}"
    # 2026.3.23 - 输入到对应业务下的目录中
    subdir = os.path.join(output_dir, parts[0])
    os.makedirs(subdir, exist_ok=True)
    out_path = os.path.join(subdir, out_name)
    out_df.to_csv(out_path, index=False, encoding="utf-8")

    print(f"已生成: {out_path} (行数 {len(out_df)}, 列数 {len(out_df.columns)})")
    return out_path

def caculate_all_filter_files(addlist, source_dir = os.path.join(script_dir,'filtered_modem_data'), output_dir = os.path.join(script_dir,'caculated_modem_data'), cmd = 'mean'):
    """
    扫描 source_dir 下 *_filtered_modem_data.csv，处理后汇总到 out_csv_path。
    """
    pattern = os.path.join(source_dir, "*_filtered_modem_data.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"未在 {source_dir} 找到 *_filtered_modem_data.csv")
        return None

    for f in files:
        try:
            print(f"处理: {f}")
            process_one_filtered_csv(f, output_dir, cmd, addlist)
        except Exception as e:
            print(f"  处理失败 {f}: {e}")

def main():
    addlist = r'D:\XFC_files\code\UDP2026\config\addlist_test.txt'
    src_dir = r'D:\XFC_files\code\UDP2026\data\processed\modem_info\filtered_modem_data'
    out_dir = r'D:\XFC_files\code\UDP2026\data\processed\modem_info\caculated_modem_data'

    caculate_all_filter_files(addlist, source_dir=src_dir, output_dir=out_dir, cmd='mean')

if __name__ == "__main__":
    main()
    
################################################# end of file