import os
import csv
import sys
import re
import datetime
import argparse
from pathlib import Path
import subprocess
import pandas as pd

import sample_ctrl_table as ctrl_table
import merge_test_withFilter as merge
import extract_UDP_features as extract
import combine_features as combine
import clean_data_operation as clean
import data_labelOperation as label

import interval_vision as vision

# 设定脚本所在目录为当前工作目录 这样可以一键启动
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)

# 后级工作的入口函数
# extract:
def extract_main(source_path, target_path):
    # 确保路径以反斜杠结尾
    if not source_path.endswith('\\') and not source_path.endswith('/'):
        source_path += '\\'
    
    if not target_path.endswith('\\') and not target_path.endswith('/'):
        target_path += '\\'
    
    # 检查目录是否存在
    if not os.path.isdir(source_path):
        print(f"错误: 源目录 '{source_path}' 不存在")
        sys.exit(1)
    
    # 确保输出目录存在
    if not os.path.isdir(target_path):
        os.makedirs(target_path)
        print(f"创建输出目录: {target_path}")
    
    # 查找send和recv文件
    send_files = [f for f in os.listdir(source_path) if f.endswith('_send.csv')]
    recv_files = [f for f in os.listdir(source_path) if f.endswith('_recv.csv')]
    
    if not send_files or not recv_files:
        print(f"错误: 在源目录 '{source_path}' 中找不到send.csv或recv.csv文件")
        sys.exit(1)
    
    # 匹配send和recv文件对
    # 假设文件名格式为xxx_send.csv和xxx_recv.csv
    file_pairs = []
    for send_file in send_files:
        # 获取基础文件名（去掉_send.csv后缀）
        base_name = send_file[:-9]  # 移除_send.csv
        # 查找对应的recv文件
        corresponding_recv = base_name + '_recv.csv'
        if corresponding_recv in recv_files:
            file_pairs.append((os.path.join(source_path, send_file), os.path.join(source_path, corresponding_recv)))
    
    if not file_pairs:
        print("错误: 找不到匹配的send和recv文件对")
        sys.exit(1)
    
    print(f"找到 {len(file_pairs)} 对文件")
    
    # 处理每一对文件
    for i, (send_file, recv_file) in enumerate(file_pairs, 1):
        print(f"处理文件对 {i}/{len(file_pairs)}: {os.path.basename(send_file)} 和 {os.path.basename(recv_file)}")
        result = extract.extract_from_oneGroup(send_file, recv_file, target_path)
        if result == -1:
            print(f"处理文件对 {i} 失败")
        else:
            print(f"文件对 {i} 处理成功")
    
    print(f"特征提取完成，结果保存在: {target_path}")

# combine:
def combine_main(extract_dir):
    featureFile_pattern = re.compile(r'extracted_([^_]+)_features.csv')
    # 检查目录是否存在
    if not os.path.exists(extract_dir):
        print(f"Error: Directory {extract_dir} does not exist")
        sys.exit(1)
    # 构建待处理文件的缓存列表
    target_files = []
    # 遍历目录下文件
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if featureFile_pattern.match(f):
                # 提取模式名
                mode_name = featureFile_pattern.match(f).group(1)
                # 构建完整的文件路径
                file_path = os.path.join(root, f)
                print(f"特征文件的来源: {mode_name}, 文件路径: {file_path}")
                # 加入缓存列表
                target_files.append(file_path)
    
    # 检查是否有目标文件
    if not target_files:
        print("错误: 没有找到匹配的特征文件")
        sys.exit(1)
    
    # 调用combine_features函数执行合并操作
    print(f"开始合并 {len(target_files)} 个特征文件...")
    combine.combine_features(target_files, extract_dir)

# clean
def clean_main(extract_dir, output_dir ,st, et, id):
    """
    !EXTRACTED_FEATURES_DIR!" "!CLEANED_DATA_DIR!" "!START_TIME_VALUE!" "!END_TIME_VALUE!" "!ID_VALUE!"
    """
    # 检查输入文件是否存在
    if not os.path.exists(extract_dir):
        print(f"Error: Input file {extract_dir} does not exist.")
        sys.exit(1)
    # 检查输出目录是否存在，不存在则创建
    if not os.path.exists(output_dir):
        print(f"Output directory {output_dir} does not exist. Creating it.")
        os.makedirs(output_dir)
    # 检查id_value是否为空
    if id == '':
        print("Error: id_value is empty.")
        sys.exit(1)
    # 从ID当中解析采样日期
    date = id[:8]
    # 检查日期格式是否正确
    if not re.match(r'\d{8}', date):
        print("Error: id_value date format is incorrect.")
        sys.exit(1)
    # 转换日期格式为YYYY-MM-DD
    date = date[:4] + '-' + date[4:6] + '-' + date[6:]
    # 将日期和起始截至时间拼接为完整时间信息
    st = date + ' ' + st
    et = date + ' ' + et
    # 转换时间字符串为datetime对象
    start_time = datetime.datetime.strptime(st, '%Y-%m-%d %H-%M-%S')
    end_time = datetime.datetime.strptime(et, '%Y-%m-%d %H-%M-%S')
    # 2025.3.16: 处理跨午夜样本 约定使用的是次日时间 开始时间回退
    if end_time < start_time:
        start_time = start_time - datetime.timedelta(days=1)
    print(f"start_time: {start_time}, end_time: {end_time}")
    clean.cleanOperation_for_oneSample(extract_dir, output_dir, start_time, end_time)

# visualize
def visualize_main(ctrl_csv):
    control_message = pd.read_csv(ctrl_csv)
    print(f'control_message: {control_message.head()}')
    
    # 遍历control_message中的每一行
    for index, row in control_message.iterrows():
        # 首先获取特征文件
        lib_add = row['lib_add']
        # 首先判断lib_add目录中cleaned_data目录是否存在
        if not os.path.exists(os.path.join(lib_add, "cleaned_data")):
            print(f"错误: 目录 '{os.path.join(lib_add, 'cleaned_data')}' 不存在。")
            continue
        featuresFile = os.path.join(lib_add, "cleaned_data", "cleaned_data.csv")
        # print(f"featuresFile: {featuresFile}")
        # 如果lag_timeList_path不是缺省值
        if not pd.isna(row['lag_timeList_path']):
            lag_timeList_path = row['lag_timeList_path']
        else:
            lag_timeList_path = ''
        print(f'lag_timeList_path: {lag_timeList_path}')
        # 检查输出路径lib_add\visualization是否存在，不存在则创建
        if not os.path.exists(os.path.join(lib_add, "visualization")):
            os.makedirs(os.path.join(lib_add, "visualization"))
        vision.visual_UDP_features(output_path = os.path.join(lib_add, "visualization"), featuresFile = featuresFile, timeListFile = lag_timeList_path)

# ip字符串的相关处理
def split_ip_string(ip_value: str) -> list[str]:
    """把 'a+b+c' 这种形式拆成 ['a','b','c']，去掉空白"""
    ip_value = (ip_value or "").strip()
    if not ip_value:
        return []
    return [p.strip() for p in ip_value.split("+") if p.strip()]

def get_ip_type(ip: str) -> str:
    """返回 'ipv4' / 'ipv6' / 'any'"""
    if ip == "*":
        return "any"
    if ":" in ip or "[" in ip:
        return "ipv6"
    return "ipv4"

def check_ip_protocol_match(ip1: str, ip2: str) -> bool:
    """等价 bat 里的 :check_ip_protocol_match"""
    if ip1 == "*" or ip2 == "*":
        return True
    return get_ip_type(ip1) == get_ip_type(ip2)

############################################################################
def process_cap_file(cap_file: Path, local_ip: str, serv_ip: str,
                     output_dir: Path, num: int) -> None:
    if not cap_file.is_file():
        raise FileNotFoundError(f"CAP file not found: {cap_file}")
    output_dir.mkdir(parents=True, exist_ok=True)

    cap_filename = cap_file.stem  # 不含后缀
    ip_format_local = get_ip_type(local_ip)
    ip_format_serv = get_ip_type(serv_ip)

    # 决定 command_mode（ipv4 / ipv6），逻辑照 bat
    command_mode = ""
    if ip_format_local == "ipv6":
        command_mode = "ipv6"
    elif ip_format_local == "ipv4":
        command_mode = "ipv4"
    elif ip_format_local == "any" and ip_format_serv == "ipv6":
        command_mode = "ipv6"
    elif ip_format_local == "any" and ip_format_serv == "ipv4":
        command_mode = "ipv4"

    if not command_mode:
        # 理论上 check_ip_protocol_match 已经过滤了这里不会来
        return
    
    # 2026.3.16：DNS基于UDPport53 我们在此也进行滤除
    udp_base_filter = "udp and not udp.port == 53"

    if command_mode == "ipv6":
        src_field = "ipv6.src"
        dst_field = "ipv6.dst"
        # 使用ipv6下 按收发区分 ：6-6 *-6 6-*
        if ip_format_local == "ipv6" and ip_format_serv == "ipv6":
            send_filter = f"{udp_base_filter} and ({src_field} == {local_ip}) and ({dst_field} == {serv_ip})"
            recv_filter = f"{udp_base_filter} and ({src_field} == {serv_ip}) and ({dst_field} == {local_ip})"
        elif ip_format_local == "ipv6" and ip_format_serv == "any":
            send_filter = f"{udp_base_filter} and ({src_field} == {local_ip})"
            recv_filter = f"{udp_base_filter} and ({dst_field} == {local_ip})"
        elif ip_format_local == "any" and ip_format_serv == "ipv6":
            send_filter = f"{udp_base_filter} and ({dst_field} == {serv_ip})"
            recv_filter = f"{udp_base_filter} and ({src_field} == {serv_ip})"
        else:
            return  # 理论上不会到这

        send_csv = output_dir / f"{cap_filename}_IPv6_send_{num}.csv"
        recv_csv = output_dir / f"{cap_filename}_IPv6_recv_{num}.csv"

    else:  # ipv4
        src_field = "ip.src"
        dst_field = "ip.dst"
        if ip_format_local == "ipv4" and ip_format_serv == "ipv4":
            send_filter = f"{udp_base_filter} and ({src_field} == {local_ip}) and ({dst_field} == {serv_ip})"
            recv_filter = f"{udp_base_filter} and ({src_field} == {serv_ip}) and ({dst_field} == {local_ip})"
        elif ip_format_local == "ipv4" and ip_format_serv == "any":
            send_filter = f"{udp_base_filter} and ({src_field} == {local_ip})"
            recv_filter = f"{udp_base_filter} and ({dst_field} == {local_ip})"
        elif ip_format_local == "any" and ip_format_serv == "ipv4":
            send_filter = f"{udp_base_filter} and ({dst_field} == {serv_ip})"
            recv_filter = f"{udp_base_filter} and ({src_field} == {serv_ip})"
        else:
            return

        send_csv = output_dir / f"{cap_filename}_IPv4_send_{num}.csv"
        recv_csv = output_dir / f"{cap_filename}_IPv4_recv_{num}.csv"

    # 输入文件 输出字段列表 第一行为表头 逗号分隔(.csv) 双引号包裹字段 
    # 输出字段-e：frame.time_epoch,ip.src,ip.dst,frame.protocols,frame.len
    base_args = [
        "tshark",
        "-r", str(cap_file),
        "-T", "fields",
        "-E", "header=y",
        "-E", "separator=,",
        "-E", "quote=d",
        "-e", "frame.time_epoch",
        "-e", src_field,
        "-e", dst_field,
        "-e", "frame.protocols",
        "-e", "frame.len",
    ]

    # 发送方向
    # eg：-Y "udp and (ip.src == 10.161.151.11) and (ip.dst == 221.178.70.26)"
    with send_csv.open("w", encoding="utf-8", newline="") as f:
        subprocess.run(base_args + ["-Y", send_filter], stdout=f, check=True)

    # 接收方向
    # -Y "udp and (ip.src == 221.178.70.26) and (ip.dst == 10.161.151.11)"
    with recv_csv.open("w", encoding="utf-8", newline="") as f:
        subprocess.run(base_args + ["-Y", recv_filter], stdout=f, check=True)

def check_ctrl_table_line(row, idx: int):
    """
    检验总表中每一行的合法性
    """
    ID_VALUE = row["samples_ID"].strip()
    SRC_ADD_VALUE = row["src_Add"].strip()
    SCENE_VALUE = row["scene"].strip()
    LIB_ADD_VALUE = row["lib_add"].strip()
    LOCAL_IP_VALUE = row["local_ip"].strip()
    SERV_IP_VALUE = row["serv_ip"].strip()
    START_TIME_VALUE = row["start_time"].strip()
    END_TIME_VALUE = row["end_time"].strip()
    CAPFILE_ADD_VALUE = row["capFile_add"].strip()

    # 1) 检查 CAPFILE_ADD_VALUE 指向的目录是否存在
    cap_dir = Path(CAPFILE_ADD_VALUE)
    if not cap_dir.is_dir():
        raise ValueError(f"控制表第 {idx} 行错误: CAPFILE_ADD_VALUE 目录不存在或不是目录: {cap_dir}")

    # 2) 目录当中是否有 .pcap* 后缀的文件存在
    if not any(cap_dir.glob("*.pcap*")):
        raise ValueError(f"控制表第 {idx} 行错误: {cap_dir} 中不包含任何 .pcap* 文件")

    # 3) 检查 SRC_ADD_VALUE 或 LIB_ADD_VALUE 的内容是否为空
    if not SRC_ADD_VALUE:
        raise ValueError(f"控制表第 {idx} 行错误: SRC_ADD_VALUE 为空")
    if not LIB_ADD_VALUE:
        raise ValueError(f"控制表第 {idx} 行错误: LIB_ADD_VALUE 为空")

    # 4) cap 源文件内容确定，检测仓库目录是否正常（LIB_ADD_VALUE 目录必须存在）
    lib_dir = Path(LIB_ADD_VALUE)
    if not lib_dir.is_dir():
        raise ValueError(f"控制表第 {idx} 行错误: LIB_ADD_VALUE 目录不存在或不是目录: {lib_dir}")

    # 5) LOCAL_IP_VALUE 至少要有内容（bat 里是 LOCAL_IP_PARTS < 1 就报错）
    if not LOCAL_IP_VALUE:
        raise ValueError(f"控制表第 {idx} 行错误: LOCAL_IP_VALUE 为空或无效")    

def process_one_sample(row, idx):
    """
    通过合法性检查后，对一行样本：
    1) 拆 IP
    2) 遍历 CAPFILE_ADD_VALUE 下所有 *.pcap*
    3) 对每个 (local_ip, serv_ip) 组合调用 tshark 导出 CSV
    """
    lib_dir = Path(row["lib_add"].strip())
    cap_dir = Path(row["capFile_add"].strip())
    local_ip_value = row["local_ip"].strip()
    serv_ip_value = row["serv_ip"].strip()

    # 拆 IP
    local_ips = split_ip_string(local_ip_value)
    if not local_ips:
        # 理论上 check_ctrl_table_line 已经拦住了
        raise ValueError(f"二次判断：第 {idx} 行 LOCAL_IP_VALUE 无有效 IP")

    # 对 serv_ip：若为空，选择当作 '*'
    serv_ips = split_ip_string(serv_ip_value) if serv_ip_value.strip() else ["*"]

    # CAP 处理结果输出目录
    caped_dir = lib_dir / "result_form_capFile"
    caped_dir.mkdir(parents=True, exist_ok=True)

    num = 0
    for cap_file in cap_dir.glob("*.pcap*"):
        # 对每个抓包文件
        for local_ip in local_ips:
            for serv_ip in serv_ips:
                if not check_ip_protocol_match(local_ip, serv_ip):
                    continue
                process_cap_file(cap_file, local_ip, serv_ip, caped_dir, num)
                num += 1

    # merge操作
    print('\033[92m\nmerge操作-正合并为send/recv两个方向的文件\033[0m')
    merge_dir = lib_dir / "merged_files"
    merge_dir.mkdir(parents=True, exist_ok=True)
    merge.test_merge(lib_path=str(caped_dir)  + os.sep, output_path=str(merge_dir)  + os.sep)

    # extract操作
    print('\033[92m\nextract操作-正通过计算提取UDP特征\033[0m')
    extract_dir = lib_dir / "extracted_features"
    extract_dir.mkdir(parents=True, exist_ok=True)
    extract_main(str(merge_dir), str(extract_dir))

    # combine操作
    print('\033[92m\ncombine操作-正进行合并操作\033[0m')
    combine_main(extract_dir)

    # clean操作
    print('\033[92m\nclean操作-正进行清洗操作（时间截取）\033[0m')
    clean_dir = lib_dir / "cleaned_data"
    clean_dir.mkdir(parents=True, exist_ok=True)
    clean_main(str(extract_dir), str(clean_dir), row["start_time"], row["end_time"], row["samples_ID"])
    

def process_ctrl_table(csv_path: str):
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"数据预处理控制总表不存在: {csv_path}")

    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)  # 用列名访问：ID, src_add, scene, ...
        rows = list(reader)
        print(f'\n控制表总行数:{len(rows)}')

    errors = []  # 收集所有分析时出问题的行

    for idx, row in enumerate(rows, start=1):
        try:
            print(f'开始处理控制表中的第{idx}行...')
            # 1) 检查合法性
            check_ctrl_table_line(row, idx)

            # 2) 如果合法，这里再继续做：拆 IP、遍历 pcap、调 tshark、后续 merge 等
            process_one_sample(row, idx)
        except Exception as e:
            # 出现错误只记录并跳过这一行，不让整个程序退出
            print(f"[WARN] 跳过第 {idx} 行: {e}")
            errors.append((idx, str(e)))
            continue 

    print(f"本次共处理 {len(rows)} 行，成功 {len(rows) - len(errors)} 行，失败 {len(errors)} 行")

    # label操作
    print('\033[92m\nlabel操作-正进行打标签操作\033[0m')
    label_files_root = r'D:\XFC_files\code\UDP2026\data_label'
    csv_path = Path(csv_path)             
    storage_dir = csv_path.parent
    label.batch_process_labels(label_files_root, storage_dir, storage_dir, 'gaming_')
    label.batch_process_labels(label_files_root, storage_dir, storage_dir, 'meeting_')
    label.batch_process_labels(label_files_root, storage_dir, storage_dir, 'video_')

        
def main():
    # 未来需要暴露的接口：addlist的位置 输出数据的位置
    parser = argparse.ArgumentParser(description="UDP样本处理工作流")
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="处理完成后根据控制表调用 interval_vision.py 进行特征可视化"
    )
    args = parser.parse_args()
    # sample_ctrl_table = r'D:\XFC_files\code\UDP2026\tests\cap_Operation\Storage\20260315_164443\csvFiles_for_CapOperation.csv'
    # 先生成一个数据预处理控制总表
    sample_ctrl_table = ctrl_table.data_PreProcessing_V1()

    if sample_ctrl_table == -1:
        print("数据预处理失败")
        exit(-1)
    print(f"已成功在Storage/下创建样本数据目录")
    print(f"已成功在Storage/下创建数据预处理总表: {sample_ctrl_table}")

    process_ctrl_table(sample_ctrl_table) 

    if args.visualize:
        print('\033[92m\n正进行可视化操作\033[0m')
        visualize_main(sample_ctrl_table)

if __name__ == "__main__":
    main()