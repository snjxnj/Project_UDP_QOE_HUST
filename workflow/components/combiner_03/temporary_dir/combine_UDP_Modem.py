import os
import re
import pandas as pd
import datetime
import glob

def process_modem_time(modem_time_str, udp_datetime):
    """
    处理Modem时间：
    1. 将时间字符串转换为datetime对象
    2. 四舍五入到秒级
    3. 使用UDP数据中的日期
    """
    # 将Modem时间字符串转换为datetime对象
    modem_dt = pd.to_datetime(modem_time_str, format='%H:%M:%S.%f')
    
    # 提取时间部分并四舍五入到秒
    hours, minutes, seconds = modem_dt.hour, modem_dt.minute, round(modem_dt.second + modem_dt.microsecond / 1e6)
    
    # 处理秒数可能的进位
    if seconds >= 60:
        seconds -= 60
        minutes += 1
        if minutes >= 60:
            minutes -= 60
            hours += 1
            if hours >= 24:
                hours -= 24
    
    # 使用UDP数据中的日期，替换时间部分
    result_dt = udp_datetime.replace(hour=hours, minute=minutes, second=seconds, microsecond=0)
    
    return result_dt

def align_data(udp_df, modem_df):
    """
    实现复杂的时间对齐规则：
    1. UDP数据中第n秒匹配Modem数据中第n+1秒
    2. 如果Modem数据由于四舍五入跳跃了1秒，UDP中未能对齐的数据将采取历史前2秒的Modem数据
    """
    # 为UDP数据创建一个新的列来存储要匹配的Modem时间
    udp_df['modem_match_time'] = udp_df['curTime_of_UTC8'] + pd.Timedelta(seconds=1)
    
    # 创建一个字典，用于快速查找Modem数据
    modem_time_dict = {}
    for _, row in modem_df.iterrows():
        # 使用四舍五入后的时间作为键
        modem_time = row['rounded_time']
        modem_time_dict[modem_time] = row
    
    # 为UDP数据添加Modem数据列
    merged_data = []
    
    for _, udp_row in udp_df.iterrows():
        udp_time = udp_row['curTime_of_UTC8']
        match_time = udp_row['modem_match_time']
        
        # 尝试匹配对应时间的Modem数据
        if match_time in modem_time_dict:
            # 找到匹配的数据
            merged_row = udp_row.to_dict()
            modem_row = modem_time_dict[match_time]
            
            # 添加Modem数据列（排除时间列）
            for col in modem_df.columns:
                if col != 'rounded_time' and col != 'timestamp':
                    merged_row[col] = modem_row[col]
            
            merged_data.append(merged_row)
        else:
            # 没有找到匹配的数据，尝试使用前2秒的Modem数据
            fallback_time = match_time - pd.Timedelta(seconds=2)
            if fallback_time in modem_time_dict:
                merged_row = udp_row.to_dict()
                modem_row = modem_time_dict[fallback_time]
                
                # 添加Modem数据列（排除时间列）
                for col in modem_df.columns:
                    if col != 'rounded_time' and col != 'timestamp':
                        merged_row[col] = modem_row[col]
                
                merged_data.append(merged_row)
            # 否则直接舍弃这条数据
    
    # 将结果转换为DataFrame
    if merged_data:
        merged_df = pd.DataFrame(merged_data)
        # 删除临时列
        if 'modem_match_time' in merged_df.columns:
            merged_df = merged_df.drop('modem_match_time', axis=1)
        return merged_df
    else:
        return None

def combine_udp_modem(udp_file, modem_file, output_dir):
    """
    融合单个UDP和Modem文件
    """
    print(f"处理文件: {os.path.basename(udp_file)} 和 {os.path.basename(modem_file)}")
    
    # 读取UDP数据
    udp_df = pd.read_csv(udp_file)
    udp_df['curTime_of_UTC8'] = pd.to_datetime(udp_df['curTime_of_UTC8'])
    
    # 读取Modem数据
    modem_df = pd.read_csv(modem_file)
    
    # 处理Modem时间
    if not udp_df.empty:
        # 使用UDP数据中的第一个日期作为基准日期
        base_datetime = udp_df['curTime_of_UTC8'].iloc[0]
        
        # 为Modem数据添加四舍五入后的时间列
        modem_df['rounded_time'] = modem_df['timestamp'].apply(
            lambda x: process_modem_time(x, base_datetime)
        )
        
        # 移除重复的时间记录（保留最后一条）
        modem_df = modem_df.drop_duplicates(subset=['rounded_time'], keep='last')
        
        # 对齐数据
        merged_df = align_data(udp_df, modem_df)
        
        if merged_df is not None and not merged_df.empty:
            # 生成输出文件名
            udp_basename = os.path.basename(udp_file)
            digit_id = get_10_digit_id(udp_basename, 'udp')
            scenario = extract_pattern(udp_basename, 'udp')
            
            output_filename = f"{scenario}_{digit_id}_combinedUDPModem.csv"
            output_path = os.path.join(output_dir, output_filename)
            
            # 保存融合后的数据
            merged_df.to_csv(output_path, index=False)
            print(f"融合完成，结果保存在: {output_filename}")
            return output_path
        else:
            print(f"没有找到可匹配的数据，跳过这对文件")
            return None
    else:
        print(f"UDP文件为空，跳过这对文件")
        return None

def extract_pattern(filename, file_type):
    """
    从文件名中提取模式字符串
    file_type: 'udp' 或 'modem'
    
    UDP文件：文件名采用下划线分隔的第0个部分是模式，第1个部分是10位ID
    Modem文件：文件名采用下划线分隔的第1个部分是模式，第2个部分是10位ID
    """
    # 分割文件名（不包含扩展名）
    name_without_ext = os.path.splitext(filename)[0]
    parts = name_without_ext.split('_')
    
    if file_type == 'udp':
        # UDP文件格式：场景_10位数字_labeled_data.csv
        # 模式是下划线分隔的第0个部分
        if len(parts) > 0:
            return parts[0]
    elif file_type == 'modem':
        # Modem文件格式：数字_场景_10位数字_mean.csv
        # 模式是下划线分隔的第1个部分
        if len(parts) > 1:
            return parts[1]
    return None

def get_10_digit_id(filename, file_type):
    """
    从文件名中提取10位数字编号
    file_type: 'udp' 或 'modem'
    
    UDP文件：文件名采用下划线分隔的第1个部分是10位ID
    Modem文件：文件名采用下划线分隔的第2个部分是10位ID
    """
    # 分割文件名（不包含扩展名）
    name_without_ext = os.path.splitext(filename)[0]
    parts = name_without_ext.split('_')
    
    if file_type == 'udp':
        # UDP文件：10位ID是下划线分隔的第1个部分
        if len(parts) > 1 and len(parts[1]) == 10 and parts[1].isdigit():
            return parts[1]
    elif file_type == 'modem':
        # Modem文件：10位ID是下划线分隔的第2个部分
        if len(parts) > 2 and len(parts[2]) == 10 and parts[2].isdigit():
            return parts[2]
    
    # 如果按照下划线分割的方式找不到，尝试使用正则表达式作为备用
    match = re.search(r'(\d{10})', filename)
    if match:
        return match.group(1)
    return None

def match_and_combine_files(udp_base_dir, modem_base_dir, output_dir):
    """
    匹配并融合所有UDP和Modem文件
    新的匹配逻辑：
    1. 遍历所有UDP源数据，记录模式字符串和对应的10位ID
    2. 遍历所有Modem源数据，记录模式字符串和对应的10位ID
    3. 遍历UDP中的模式和ID，在Modem中对应模式下寻找对应ID，匹配成功则生成匹配对
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 步骤1：遍历所有UDP文件，提取模式和10位ID
    udp_dict = {}
    udp_files = []
    
    # 遍历所有场景的UDP文件
    for scenario in ['gaming', 'meeting', 'video']:
        udp_dir = os.path.join(udp_base_dir, scenario)
        scenario_udp_files = glob.glob(os.path.join(udp_dir, f"{scenario}_*_labeled_data.csv"))
        udp_files.extend(scenario_udp_files)
    
    print(f"找到 {len(udp_files)} 个UDP文件")
    
    for udp_file in udp_files:
        filename = os.path.basename(udp_file)
        pattern = extract_pattern(filename, 'udp')
        digit_id = get_10_digit_id(filename, 'udp')
        
        if pattern and digit_id:
            if pattern not in udp_dict:
                udp_dict[pattern] = {}
            udp_dict[pattern][digit_id] = udp_file
    
    # 统计UDP模式数量
    udp_pattern_count = len(udp_dict)
    print(f"UDP数据中有 {udp_pattern_count} 个模式: {list(udp_dict.keys())}")
    for pattern, ids in udp_dict.items():
        print(f"  模式 {pattern} 下有 {len(ids)} 个ID")
    
    # 步骤2：遍历所有Modem文件，提取模式和10位ID
    modem_dict = {}
    modem_files = []
    
    # 遍历所有场景的Modem文件
    for scenario in ['gaming', 'meeting', 'video']:
        modem_dir = os.path.join(modem_base_dir, scenario)
        scenario_modem_files = glob.glob(os.path.join(modem_dir, f"*_{scenario}_*_mean.csv"))
        modem_files.extend(scenario_modem_files)
    
    print(f"\n找到 {len(modem_files)} 个Modem文件")
    
    for modem_file in modem_files:
        filename = os.path.basename(modem_file)
        pattern = extract_pattern(filename, 'modem')
        digit_id = get_10_digit_id(filename, 'modem')
        
        if pattern and digit_id:
            if pattern not in modem_dict:
                modem_dict[pattern] = {}
            modem_dict[pattern][digit_id] = modem_file
    
    # 统计Modem模式数量
    modem_pattern_count = len(modem_dict)
    print(f"Modem数据中有 {modem_pattern_count} 个模式: {list(modem_dict.keys())}")
    for pattern, ids in modem_dict.items():
        print(f"  模式 {pattern} 下有 {len(ids)} 个ID")
    
    # 步骤3：开始匹配
    print("\n开始匹配文件...")
    matched_count = 0
    
    for pattern, udp_ids in udp_dict.items():
        print(f"\n处理模式: {pattern}")
        
        if pattern in modem_dict:
            modem_ids = modem_dict[pattern]
            
            for digit_id, udp_file in udp_ids.items():
                if digit_id in modem_ids:
                    modem_file = modem_ids[digit_id]
                    combine_udp_modem(udp_file, modem_file, output_dir)
                    matched_count += 1
                else:
                    print(f"  模式 {pattern} 下的ID {digit_id} 在Modem数据中未找到匹配")
        else:
            print(f"  模式 {pattern} 在Modem数据中未找到匹配")
    
    print(f"\n匹配完成，共找到 {matched_count} 对匹配的文件")

def main():
    # 设置基础目录
    UDP_BASE_DIR = "d:\\General_Workspace\\Workspace-of-UDP-NEW\\DataDir\\2025_12_9_FixedDataSheet\\Month10_11\\UDP"
    MODEM_BASE_DIR = "d:\\General_Workspace\\Workspace-of-UDP-NEW\\DataDir\\2025_12_9_FixedDataSheet\\Month10_11\\Modem"
    OUTPUT_DIR = "d:\\General_Workspace\\Workspace-of-UDP-NEW\\DataDir\\2025_12_9_FixedDataSheet\\Month10_11\\merged_UDP_Modem\\combine_workspace\\combined_data"
    
    print("开始融合UDP和Modem数据...")
    match_and_combine_files(UDP_BASE_DIR, MODEM_BASE_DIR, OUTPUT_DIR)
    print("\n所有文件处理完成！")

if __name__ == "__main__":
    main()