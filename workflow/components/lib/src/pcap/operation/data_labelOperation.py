import os
import re
import pandas as pd
import datetime

interval_pattern = r'^([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})-([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})$'
time_withPoint_pattern = r'^([0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}\.[0-9]{3})$'
time_pattern = r'^([0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2})$'

def makeLabels_for_OneSample(featuresFile, timeListFile, outputDir=None):
    """
    为单个样本添加标签
    
    Args:
        featuresFile: 特征文件路径
        timeListFile: 时间列表文件路径
        outputDir: 输出目录，如果为None则使用原文件目录
    """
    # 读取特征文件
    features_df = pd.read_csv(featuresFile)
    features_df['label'] = 0
    
    # 从timeListFile文件名中提取日期信息
    # 只取文件名部分，忽略路径
    filename = os.path.basename(timeListFile)
    parts = filename.split('_')
    # 业务场景
    env_part = parts[0]  # 如 "gaming"
    # 日期在第三部分（如 "20251102"），可能后面还有额外的数字后缀
    date_part = parts[3]  # 如 "2025110209"
    date = date_part[:8]  # 取前8位作为日期 "20251102"
    # 构建新的时间区间表
    new_timeList = []
    # 读取时间列表信息
    with open(timeListFile, 'r') as f:
        lines = f.readlines()
    # 逐行遍历时间信息
    for line in lines:
        line = line.strip()
        if line == "":
            continue
        if re.match(interval_pattern, line):
            # 获取每一行时间区间的起始时刻和截至时刻
            start_time, end_time = re.match(interval_pattern, line).groups()
            # 结合日期信息构建完整的时间信息
            start_time_dt = datetime.datetime.strptime(date + ' ' + start_time, '%Y%m%d %H:%M:%S.%f')
            end_time_dt = datetime.datetime.strptime(date + ' ' + end_time, '%Y%m%d %H:%M:%S.%f')
            # 开始对齐时间区间的起始时刻和截至时刻
            aligned_startTime_dt = start_time_dt.replace(microsecond=0)
            if(start_time_dt.microsecond >= 500000):    aligned_startTime_dt += datetime.timedelta(seconds=1)
            aligned_endTime_dt = end_time_dt.replace(microsecond=0)
            if(end_time_dt.microsecond >= 500000):    aligned_endTime_dt += datetime.timedelta(seconds=1)
            # 如果四舍五入的结果显示时间区间消失了，则忽视这个卡顿瞬间
            if aligned_startTime_dt == aligned_endTime_dt:
                continue
            # 构建新的时间区间表
            new_timeList.append({
                "start_time": aligned_startTime_dt.strftime('%H:%M:%S.%f'),
                "end_time": aligned_endTime_dt.strftime('%H:%M:%S.%f'),
                "time_interval": (aligned_endTime_dt - aligned_startTime_dt).total_seconds()
            })
        elif re.match(time_pattern, line):
            # 匹配单一时刻点，并且没有小数位，说明是直接的标记信息
            label_time = re.match(time_pattern, line).groups()[0]
            # 结合日期信息构建完整的时间信息
            label_time_dt = datetime.datetime.strptime(date + ' ' + label_time, '%Y%m%d %H:%M:%S')
            # 构建新的时间区间表
            new_timeList.append({
                "start_time": label_time_dt.strftime('%H:%M:%S.%f'),
                "end_time": label_time_dt.strftime('%H:%M:%S.%f'),
                "time_interval": 0
            })
    # 转换为DataFrame
    new_timeList_df = pd.DataFrame(new_timeList)
    
    # 开始根据新的时间区间表为特征文件添加标签
    for index, row in features_df.iterrows():
        curTime_of_UTC8 = datetime.datetime.strptime(row['curTime_of_UTC8'], '%Y-%m-%d %H:%M:%S')
        curTime_of_UTC8 = curTime_of_UTC8.time()
        for index2, row2 in new_timeList_df.iterrows():
            startTime_of_UTC8 = datetime.datetime.strptime(row2['start_time'], '%H:%M:%S.%f').time()
            endTime_of_UTC8 = datetime.datetime.strptime(row2['end_time'], '%H:%M:%S.%f').time()
            if(curTime_of_UTC8 >= startTime_of_UTC8 and curTime_of_UTC8 < endTime_of_UTC8):
                features_df.at[index, 'label'] = 1
                break
    
    # 从特征文件路径中提取样本编号
    features_dir = os.path.dirname(featuresFile)
    sample_dir = os.path.basename(features_dir)  # 获取特征文件所在目录的父目录名称
    # print(f"!!!sample_dir!!!: {sample_dir}")
    # 从样本目录名中提取编号（忽略末尾5位数字）
    sample_id = extract_sample_id_from_dirname(sample_dir)
    
    # 确定输出路径
    if outputDir:
        # 确保输出目录存在
        os.makedirs(outputDir, exist_ok=True)
        if date_part:
            # 2026.1.13: 前面加上0000_减少后续train/test人工划分工作
            # 2026.3.20: 划分训练/测试集用后续脚本控制吧
            output_file = os.path.join(outputDir, f"{env_part}_{date_part}_labeled_data.csv")
        else:
            output_file = os.path.join(outputDir, "labeled_data.csv")
    else:
        # 使用原文件目录
        base_dir = os.path.dirname(featuresFile)
        outputDir = os.path.join(base_dir, "labeled_data")
        os.makedirs(outputDir, exist_ok=True)
        if date_part:
            output_file = os.path.join(outputDir, f"{env_part}_{date_part}_labeled_data.csv")
        else:
            output_file = os.path.join(outputDir, "labeled_data.csv")
    
    features_df.to_csv(output_file, index=False)
    print(f"Labeled file saved as: {output_file}")
    return output_file


def extract_sample_id_from_dirname(dirname):
    """
    从子目录名称中提取样本编号，忽略末尾的5位数字
    
    Args:
        dirname: 子目录名称，如 "gaming_202510170100110_export_Time20251017_211351"
    
    Returns:
        str: 提取的样本编号，如 "2025101701"
    """
    parts = dirname.split('_')
    if len(parts) >= 2:
        # 获取第二段并移除末尾的5位数字
        second_part = parts[1]  # 如 "202510170100110"
        if len(second_part) > 5:
            # 移除末尾5位数字
            sample_id = second_part[:-5]  # 如 "2025101701"
            return (parts[0]+sample_id)
    return None


def extract_sample_id_from_txtfile(txt_filename):
    """
    从txt文件名中提取样本编号
    
    Args:
        txt_filename: txt文件名，如 "gaming_sgood_sgood_2025101701_lag_timeList.txt"
    
    Returns:
        str: 提取的样本编号，如 "2025101701"
    """
    parts = txt_filename.split('_')
    if len(parts) >= 4:
        return (parts[0]+parts[3])  # 第四段是编号
    return None


def batch_process_labels(label_root_dir, sample_root_dir, script_dir, del_env):
    """
    递归批量处理所有样本的标签

    Args:
        label_root_dir: 标签txt文件所在根目录（将递归查找所有 *_lag_timeList.txt）
        sample_root_dir: 样本目录所在根目录（将递归查找所有以 gaming_ 开头的目录）
        del_env: 业务场景前缀，如 "video_"
    """
    print(f"开始批量处理标签")
    print(f"标签根目录: {label_root_dir}")
    print(f"样本根目录: {sample_root_dir}")

    # 递归获取所有txt文件
    txt_files = []
    for root, _, files in os.walk(label_root_dir):
        for f in files:
            if f.endswith('_lag_timeList.txt'):
                txt_files.append(os.path.join(root, f))

    # 递归获取所有子目录（gaming开头的目录）
    subdirs = []
    for root, dirs, _ in os.walk(sample_root_dir):
        for d in dirs:
            if d.startswith(del_env):
                subdirs.append(os.path.join(root, d))

    print(f"找到 {len(txt_files)} 个时间列表文件")
    print(f"找到 {len(subdirs)} 个子目录")

    # 创建匹配字典
    matches = []
    
    # 将子目录映射为 {提取的编号: 目录路径}
    subdir_map = {}
    for subdir_path in subdirs:
        subdir_name = os.path.basename(subdir_path)
        subdir_id = extract_sample_id_from_dirname(subdir_name)
        if subdir_id:
            subdir_map.setdefault(subdir_id, []).append(subdir_path)

    for txt_path in txt_files:
        txt_filename = os.path.basename(txt_path)
        txt_id = extract_sample_id_from_txtfile(txt_filename)
        if not txt_id:
            continue
        # 找到对应的子目录（可能多个，通常一个）
        candidate_dirs = subdir_map.get(txt_id, [])
        for subdir in candidate_dirs:
            matches.append({
                'txt_path': txt_path,
                'subdir_path': subdir,
                'txt_id': txt_id,
            })

    print(f"找到 {len(matches)} 个匹配的样本")
    for match in matches:
        print(f"匹配: TXT[{match['txt_id']}] <-> DIR[{os.path.basename(match['subdir_path'])}]")

    # 处理每个匹配的样本
    success_count = 0
    for match in matches:
        txt_path = match['txt_path']
        subdir_path = match['subdir_path']

        cleaned_data_path = os.path.join(subdir_path, 'cleaned_data', 'cleaned_data.csv')
        # 2026.1.13: 输出目录改为 script_dir/labeled_data/<业务场景>/
        # 2026.3.10：传入script_dir/Storage/操作时间/   labeled_data/<业务场景>/
        output_dir = os.path.join(script_dir, 'labeled_data', del_env.rstrip('_'))

        # 检查文件是否存在
        if not os.path.exists(cleaned_data_path):
            print(f"警告: 找不到文件 {cleaned_data_path}")
            continue
        if not os.path.exists(txt_path):
            print(f"警告: 找不到文件 {txt_path}")
            continue

        print(f"处理样本 {subdir_path}...")
        try:
            output_file = makeLabels_for_OneSample(cleaned_data_path, txt_path, output_dir)
            success_count += 1
            print(f"成功处理: {subdir_path} -> {output_file}")
        except Exception as e:
            print(f"处理 {subdir_path} 时出错: {str(e)}")

    print(f"批量处理完成，成功处理 {success_count}/{len(matches)} 个'{del_env}'样本")
    return success_count


if __name__ == '__main__':
    # 当前脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # 允许用户自定义两个路径；若未设置则使用默认推导路径
    label_files_root = r'D:\XFC_files\code\UDP_QoE\data_label'
    # features_files_root = os.path.join(script_dir, '20251209_120455')
    features_files_root = r'D:\XFC_files\code\UDP_QoE\data_PreProcessing\Storage\gaming'

    batch_process_labels(label_files_root, features_files_root, script_dir, 'gaming_')
    # batch_process_labels(label_files_root, features_files_root, script_dir, 'meeting_')
    # batch_process_labels(label_files_root, features_files_root, script_dir, 'video_')
