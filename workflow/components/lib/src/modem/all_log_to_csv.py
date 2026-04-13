import re
import json
import csv
import os
import glob
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import src.add_list.Timelabel as timer
import src.add_list.Listparser as listparser
#################################################
"""
该脚本主要负责从原始的all.log_文件中提取modem相关数据，并从生成的CSV文件中过滤出需要的特征列
初步提取的特征列为字符串形式，未拆分5项取值，如："mn_d_r":"37,27,32,38,63"

2026.1.26 此后生成的文件均为按照addlist中起止时间截取后的文件
"""
#################################################

script_dir = os.path.dirname(os.path.abspath(__file__))

#################################################
# 根据上表对原始提取到的数据特征进行过滤
ALLOWED_FEATURES = [
    # 初阶段使用的数据 - 21个
    "mn_u_r","mn_d_r","mn_u_m","mn_d_m","mn_u_g","mn_d_g",
    "mn_u_b","mn_d_b","mn_txp_md","mn_cqi","mn_mac",
    "mn_pu_m_l","mn_pd_m_l","mn_pp_r","mn_ssnr","mn_tsnr",
    "mn_path_loss","mn_target_pwr","mn_bandwidth",
    "mn_rsrp_4","mn_snr_4",

    # 补充剩下的数据特征 - 20个
    "mn_cc","mn_redrt","mn_rre_count","mn_rach_fail","mn_rlf_num","mn_ota_irat","mn_irat",
    "mn_mtpl","mn_rlf_cause","mn_retr","mn_pp_s","mn_pp_dis","mn_ri","mn_prach_cfg","mn_pre_for",
    "mn_o_fail","mn_ph_dis","rf_sul_state","rf_sul_band","mn_ver"
    # 最终输出带时间戳的总列数:21 + 20 - 2 + 2 * 4 + 1 = 48
]

def filter_modem_data(df: pd.DataFrame) -> pd.DataFrame:
    # timestamp 保留，忽略大小写差异的 mn_bandwith / mn_bandwidth
    cols = df.columns
    keep = []
    for c in cols:
        if c == "timestamp":
            keep.append(c)
        elif c in ALLOWED_FEATURES:
            keep.append(c)
        elif c == "mn_bandwidth" and "mn_bandwith" in ALLOWED_FEATURES:
            keep.append(c)
    return df[keep]

def find_all_log_files(root_dir="."):
    """
    在给定根目录下递归查找所有以 all.log_ 开头的文件
    返回 [(文件完整路径, 父目录名), ...]
    """
    results = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.startswith("all.log_"):
                fullpath = os.path.join(dirpath, filename)
                parent_name = os.path.basename(dirpath)
                results.append((fullpath, parent_name))
    print(f"在 {os.path.abspath(root_dir)} 下找到 {len(results)} 个 all.log_ 文件:")
    for fullpath, parent in results:
        print(f"  - {fullpath} (父目录: {parent})")
    return results

def group_all_logs_by_parent(root_dir="."):
    """
    将 find_all_log_files 的结果按父目录分组:
    返回 dict[parent_dir_name] = [file_path1, file_path2, ...]
    """
    grouped = defaultdict(list)
    files_with_parents = find_all_log_files(root_dir=root_dir)
    for fp, parent in files_with_parents:
        grouped[parent].append(fp)
    print("分组结果：")
    for parent, fps in grouped.items():
        print(f"  父目录 {parent} -> {len(fps)} 个日志")
    return grouped

def compute_group_key(parent_dir_name: str) -> str:
    """
    从父目录名生成会话键，用于合并。
    约定：test_meeting_2025103101----parent_dir_name => meeting_2025103101
    若不满足三段，下沉为安全父目录名。
    """
    parts = parent_dir_name.split('_')
    if len(parts) >= 3:
        return f"{parts[1]}_{parts[2]}"
    return parent_dir_name.replace(' ', '_')

def _extract_balanced_json(line, start_idx):
    """
    从行内 start_idx 的 '{' 开始，按大括号与字符串状态平衡提取一个 JSON 子串。
    返回 json_str 或 None
    """
    depth = 0
    in_string = False
    escape = False
    for i, ch in enumerate(line[start_idx:], start=start_idx):
        if escape:
            escape = False
            continue
        if ch == '\\':
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if not in_string:
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return line[start_idx:i+1]
    return None

def parse_single_log_file(log_file_path, parent_dir_name,output_dir):
    """
    解析单个日志文件，生成对应的CSV文件
    命名：test_meeting_2025103101----parent_dir_name
    则返回命名：meeting_2025103101_modem_data.csv
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 生成输出文件名（加入当前log名，避免同会话多个log相互覆盖）
    parts = parent_dir_name.split('_')
    base_name = os.path.splitext(os.path.basename(log_file_path))[0]  # 例：all.log_1
    safe_parent = parent_dir_name.replace(' ', '_')
    if len(parts) >= 3:
        key = f"{parts[1]}_{parts[2]}"
    else:
        key = safe_parent
    csv_file = os.path.join(output_dir, f"{key}_{base_name}_modem_data.csv")
    # 仅匹配时间戳与 printModemInfo 标记，JSON 采用括号平衡提取
    pattern = r'(\d{2}:\d{2}:\d{2}\.\d{3}).*?printModemInfo\s+\d+\s+'
    pattern_data = r'(\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'
    
    modem_data = []
    print(f"正在解析文件: {log_file_path}")
    
    # 从父目录名 parts[2] 提取年份（如 2025120301 -> 2025）
    base_year = None
    if len(parts) >= 3:
        m_date = re.search(r'(20\d{6})', parts[2])  # 匹配前8位日期 20251203
        if m_date:
            base_year = m_date.group(1)[:4]        # 只要年份 '2025'

    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as log_file:
        for line_num, line in enumerate(log_file, 1):
            m = re.search(pattern, line)
            if not m:
                continue

            # 只在 printModemInfo 前面的部分抓日期+时间
            idx_print = line.find("printModemInfo")
            prefix = line[:idx_print] if idx_print != -1 else line

            m_data = re.search(pattern_data, prefix)
            if not m_data:
                # 如果按你的数据规范，所有行都有 "10-17 20:59:42.777 ..."，这里也可以直接 continue
                time_str = m.group(1)          # 兜底只用时间
                date_md = None
            else:
                date_md = m_data.group(1)      # "10-17"
                time_str = m_data.group(2)     # "20:59:42.777"

            # 组合最终日期时间：base_year + date_md + time_str
            timestamp_full = time_str
            if base_year is not None and date_md is not None:
                dt_str = f"{base_year}-{date_md} {time_str}"  # 例如 2025-10-17 20:59:42.777
                try:
                    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                # 保留到毫秒
                timestamp_full = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            brace_start = line.find('{', m.end())
            if brace_start == -1:
                continue
            json_str = _extract_balanced_json(line, brace_start)
            if not json_str:
                last_brace = line.rfind('}')
                if last_brace != -1 and last_brace > brace_start:
                    json_str = line[brace_start:last_brace+1]
                else:
                    print(f"  第{line_num}行JSON括号不平衡，跳过")
                    continue
            try:
                data = json.loads(json_str)
                data['timestamp'] = timestamp_full
                data['line_number'] = line_num
                modem_data.append(data)
            except json.JSONDecodeError as e:
                print(f"  第{line_num}行JSON解析错误: {e}")
                continue
    
    if not modem_data:
        print(f"  在 {log_file_path} 中未找到有效的modem数据")
        return None
    
    # 写入CSV文件（第一列为时间戳；其他字段原样写入，暂时不拆分 5 项值，在process_modem_features.py脚本进行数值计算处理）
    fieldnames = ['timestamp', 'line_number'] + [k for k in modem_data[0].keys() if k not in ['timestamp', 'line_number']]
    with open(csv_file, 'w', newline='', encoding='utf-8') as csv_output:
        writer = csv.DictWriter(csv_output, fieldnames=fieldnames)
        writer.writeheader()
        for data in modem_data:
            row = {'timestamp': data['timestamp'], 'line_number': data['line_number']}
            for key in fieldnames[2:]:
                value = data.get(key, '')
                row[key] = '' if value == '' else str(value)
            writer.writerow(row)
    
    print(f"  CSV文件已生成: {csv_file}")
    return csv_file

def filter_all_generated_csv(source_dir, target_dir = os.path.join(script_dir, "filtered_modem_data"), addlist = None):
    parser = listparser.SampleListParser(addlist) if addlist is not None else None

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    pattern = os.path.join(source_dir, "*_modem_data.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"过滤阶段：未在 {source_dir} 发现 *_modem_data.csv")
        return
    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8")
            f_df = filter_modem_data(df)
            # 输出文件名添加 _filtered
            base = os.path.basename(f)
            parts = base.split('_')
            sample_key = parts[0] + '_' + parts[1]
            # 判断是否需要时间截取
            if parser is not None:
                sample_info = parser.get_sample_info(sample_key)
                if sample_info and 'start_time' in sample_info and 'end_time' in sample_info:
                    st = sample_info['start_time']
                    et = sample_info['end_time']
                    f_df = timer.cut_df_by_time(f_df, start_time=st, end_time=et)
                else:
                    print(f"警告：addlist未找到 {sample_key} 的时间区间，跳过时间截取")
            else:
                print("警告：未提供 addlist，跳过所有时间截取")
            out_path = os.path.join(target_dir, base.replace("_modem_data.csv", "_filtered_modem_data.csv"))
            f_df.to_csv(out_path, index=False, encoding="utf-8")
            print(f"已过滤并生成: {out_path} (保留列数 {len(f_df.columns)})")
        except Exception as e:
            print(f"过滤文件失败 {f}: {e}")

#################################################
# 处理最原始的all.log_文件，先提取modem数据并生成CSV，后过滤不相关列
def process_all_log_files(data_set_root_dir, addlist:str, output_dir = os.path.join(script_dir, "extract_from_all_logs")):
    """
    主函数：按父目录分组，合并其下所有 all.log_ 为一个 CSV
    """
    print("开始自动检索all.log开头的日志文件...")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 分组结构：父目录名 -> all.log_ 文件列表
    parent_group_map = group_all_logs_by_parent(root_dir=data_set_root_dir)
    if not parent_group_map:
        print("未找到任何all.log开头的日志文件！")
        return

    processed_groups = []
    for parent_dir_name, log_files in parent_group_map.items():
        if not log_files:
            continue
        print(f"\n处理父目录: {parent_dir_name}，包含 {len(log_files)} 个日志")
        dfs = []
        for log_file_path in sorted(log_files):
            try:
                # 解析单个日志，返回临时分片 CSV 路径
                tmp_csv = parse_single_log_file(log_file_path, parent_dir_name, output_dir)
                if not tmp_csv:
                    continue
                # 直接读取分片 CSV 为 DataFrame，加入待合并列表
                df = pd.read_csv(tmp_csv, encoding="utf-8")
                dfs.append(df)
                # 解析成功后可删除分片文件，避免堆积
                try:
                    os.remove(tmp_csv)
                except Exception:
                    pass
            except Exception as e:
                print(f"  处理文件 {log_file_path} 时出错: {e}")

        if not dfs:
            print(f"  父目录 {parent_dir_name} 下无有效数据，跳过")
            continue

        merged = pd.concat(dfs, axis=0, ignore_index=True, sort=False)
        # 去重与排序
        if "timestamp" in merged.columns:
            merged = merged.drop_duplicates(subset=["timestamp"], keep="first")
            merged = merged.sort_values(by="timestamp", kind="stable")

        # 列顺序：timestamp, line_number, 其他
        cols = list(merged.columns)
        ordered = []
        for c in ["timestamp", "line_number"]:
            if c in cols:
                ordered.append(c)
        for c in cols:
            if c not in ordered:
                ordered.append(c)
        merged = merged.reindex(columns=ordered)

        # 使用会话键作为输出文件名（来源于父目录名）
        group_key = compute_group_key(parent_dir_name)
        merged_csv_path = os.path.join(output_dir, f"{group_key}_modem_data.csv")
        merged.to_csv(merged_csv_path, index=False, encoding="utf-8")
        print(f"  合并完成: {merged_csv_path}  (行数={len(merged)})")
        
        processed_groups.append(merged_csv_path)

    # 对合并结果进行特征过滤
    filter_all_generated_csv(
        source_dir=output_dir,
        target_dir=os.path.join(os.path.dirname(output_dir), "filtered_modem_data"),
        addlist=addlist
    )

    print(f"\n处理完成！共成功生成 {len(processed_groups)} 个合并后的 CSV")
    for file in processed_groups:
        print(f"  - {file}")

if __name__ == "__main__":
    # 单脚本测试
    data_set_root_dir = r'D:\XFC_files\code\UDP2026\data\raw\Modem'

    process_all_log_files(data_set_root_dir,
                          addlist = r'D:\XFC_files\code\UDP2026\config\addlist_test.txt',
                          output_dir=r'D:\XFC_files\code\UDP2026\data\processed\modem_info\csv_from_all_logs')

################################################# end of file