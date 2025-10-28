import os
import re
import pandas as pd
import ipaddress
import sys

# 如果需要更改send和recv文件的正则匹配方式，则修改此处
# 匹配用户提供的文件名格式: tcp_dump_2025_1017_205506_IPv4_recv_0 或 tcp_dump_2025_1017_205506_IPv6_recv_0
recv_pattern = re.compile(r'tcp_dump_\d{4}_\d{2}\d{2}_\d{6}_IPv[46]_recv_\d+\.csv')
send_pattern = re.compile(r'tcp_dump_\d{4}_\d{2}\d{2}_\d{6}_IPv[46]_send_\d+\.csv')
# IPv6地址的正则匹配规则
ipv6_pattern = re.compile(r'^'
    r'(?:'
    r'(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|'
    r'(?:[0-9a-fA-F]{1,4}:){1,7}:|'
    r'(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|'
    r'(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|'
    r'(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|'
    r'(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|'
    r'(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|'
    r'[0-9a-fA-F]{1,4}:(?::[0-9a-fA-F]{1,4}){1,6}|'
    r':(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|'
    r'fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]+|'
    r'::(?:ffff(?::0{1,4})?:)?(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|'
    r'(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)'
    r')$')
# 定义IPv4地址的正则匹配规则
ipv4_pattern = re.compile(r'^'
    r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
    r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
    r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.'
    r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')

def is_valid_IPv6Add(ipv6_add):
    try:
        ipaddress.IPv6Address(ipv6_add)
        return True
    except ValueError:
        return False

def is_valid_IPv6Add_V1(ipv6_add):
    if ipv6_pattern.match(ipv6_add):
        return True
    else:
        return False

"""
过滤IPv6地址的函数
参数:
    data_df: 包含IPv6地址的DataFrame
返回:
    过滤后的DataFrame，仅包含合法的IPv6地址
"""
def filter_IPv6Add(data_df):
    # 数据的非法性判断
    if data_df is None or data_df.empty:
        return data_df
    # 检测过滤目标的存在性
    filter_target = ['ipv6.src', 'ipv6.dst']
    for col in filter_target:
        if col not in data_df.columns:
            print("Error: " + col + " not in data_df")
            return data_df
    # 开始过滤操作
    filtered_df = data_df[(data_df['ipv6.src'].apply(is_valid_IPv6Add)) & (data_df['ipv6.dst'].apply(is_valid_IPv6Add))]
    # 过滤结果返回
    return filtered_df

"""
测试合并recv和send文件的函数
参数:
    lib_path: 包含recv和send文件的目录路径
    output_path: 合并输出文件的目录路径
    ID: 测试ID，默认值为1111
    time: 测试时间，默认值为"20000101-000000"
返回:
    无
"""
def test_merge(lib_path, output_path, ID = 1111, time = "20000101-000000"):
    recv_result = pd.DataFrame()
    send_result = pd.DataFrame()

    recv_files = [f for f in os.listdir(lib_path) if recv_pattern.match(f)]
    send_files = [f for f in os.listdir(lib_path) if send_pattern.match(f)]
    # print(recv_files)
    # print(send_files)

    for recv_file in recv_files:
        # 如果recv_file不存在，则跳过
        if recv_file not in recv_files:
            continue
        recv_cache = pd.read_csv(os.path.join(lib_path, recv_file))
        # print(recv_cache)
        try:
            # 如果recv_file只有一行，则跳过
            if len(recv_cache) == 1:
                continue
            # 如果recv_cache列名中包含了"ipv6.src"和"ipv6.dst"
            if "ipv6.src" in recv_cache.columns and "ipv6.dst" in recv_cache.columns:
                #将列名"ipv6.src"改为"ip.src"
                recv_cache.rename(columns={"ipv6.src": "ip.src"}, inplace=True)
                #将列名"ipv6.dst"改为"ip.dst"
                recv_cache.rename(columns={"ipv6.dst": "ip.dst"}, inplace=True)
            recv_result = pd.concat([recv_result, recv_cache], ignore_index=True)
        except:
            print("Error in recv file: " + recv_file + ", ID: " + str(ID) + ", Time: " + time)
            continue
    for send_file in send_files:
        # 如果send_file不存在，则跳过
        if send_file not in send_files:
            continue
        send_cache = pd.read_csv(os.path.join(lib_path, send_file))
        # print(send_cache)
        try:
            # 如果send_file只有一行，则跳过
            if len(send_cache) == 1:
                continue
            # 如果send_cache列名中包含了"ipv6.src"和"ipv6.dst"
            if "ipv6.src" in send_cache.columns and "ipv6.dst" in send_cache.columns:
                #将列名"ipv6.src"改为"ip.src"
                send_cache.rename(columns={"ipv6.src": "ip.src"}, inplace=True)
                #将列名"ipv6.dst"改为"ip.dst"
                send_cache.rename(columns={"ipv6.dst": "ip.dst"}, inplace=True)
            send_result = pd.concat([send_result, send_cache], ignore_index=True)
        except:
            print("Error in send file: " + send_file + ", ID: " + str(ID) + ", Time: " + time)
            continue
    # 按照时间顺序进行排序
    recv_result_sorted = recv_result.sort_values(by="frame.time_epoch", ascending=True)
    send_result_sorted = send_result.sort_values(by="frame.time_epoch", ascending=True)
    # print(recv_result_sorted)
    # print(send_result_sorted)

    try:
        recv_result_sorted.to_csv(output_path + "merged_recv" + ".csv", index=False)
        send_result_sorted.to_csv(output_path + "merged_send" + ".csv", index=False)
        print(f"合并文件已保存到: {output_path}")
    except Exception as e:
        print("Error in merge file: " + lib_path)
        print(f"错误详情: {str(e)}")
        return



if __name__ == "__main__":
    # 从命令行获取目录路径参数
    # 第一个参数是输入目录(CAPED_FILE_DIR)
    # 第二个参数是输出目录(MERGED_FILE_DIR)
    if len(sys.argv) >= 3:
        # 输入目录 - 源文件所在目录
        lib_path = sys.argv[1]
        # 输出目录 - 合并文件保存目录
        output_path = sys.argv[2]
    elif len(sys.argv) == 2:
        # 如果只有一个参数，将其作为输入目录，输出目录默认为输入目录
        lib_path = sys.argv[1]
        output_path = sys.argv[1]
    else:
        # 默认使用当前目录
        lib_path = ".\\"
        output_path = ".\\"
    
    # 确保目录路径以反斜杠结尾
    if not lib_path.endswith("\\") and not lib_path.endswith("/"):
        lib_path += "\\"
    if not output_path.endswith("\\") and not output_path.endswith("/"):
        output_path += "\\"
    
    print(f"输入目录: {lib_path}")
    print(f"输出目录: {output_path}")
    test_merge(lib_path=lib_path, output_path=output_path)
