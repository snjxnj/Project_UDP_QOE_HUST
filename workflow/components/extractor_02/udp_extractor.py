import os
import re
import sys
import pandas as pd
import numpy as np
import time
import datetime as dt
from ipaddress import ip_address, IPv6Address, IPv4Address
from .extractor import Extractor

def is_ipv6(addr: str) -> bool:
    """判断字符串是否为合法的IPv6地址"""
    try:
        ip_obj = ip_address(addr)
        return isinstance(ip_obj, IPv6Address)
    except ValueError:
        return False

def is_ipv4(addr: str) -> bool:
    """判断字符串内容是否为合法的IPv4地址"""
    try:
        ip_obj = ip_address(addr)
        return isinstance(ip_obj, IPv4Address)
    except ValueError:
        return False

def get_IPpair_from_line(line: str):
    local_ip = None
    serv_ip = None
    if "<->" in line:
        parts = line.strip().split()
        ip_1, port_1 = parts[0].rsplit(":", 1)
        ip_2, port_2 = parts[2].rsplit(":", 1)
        if ((is_ipv4(ip_1) or is_ipv6(ip_1))
            and (is_ipv4(ip_2) or is_ipv6(ip_2))):
            local_ip = ip_1
            serv_ip = ip_2

    return local_ip, serv_ip

def transfer_to_B(num: int, unit: str):
    # 给出负载的单位转换-统一转换到字节单位
    units = {
        "B":    1,
        "KB":   1024,
        "kB":   1024,
        "MB":   1024*1024,
        "GB":   1024*1024*1024
    }
    return (num * units[unit]) if unit in units else -1

def get_load_from_line(line: str):
    down_load = -1
    up_load = -1
    total_load = -1
    if "<->" in line:
        parts = line.strip().split()
        down_load = transfer_to_B(int(parts[4]), parts[5])
        up_load = transfer_to_B(int(parts[7]), parts[8])
        total_load = transfer_to_B(int(parts[10]),parts[11])
    return down_load, up_load, total_load


"""
@brief 类udp_extractor，继承自Extractor虚拟父类
"""
class udp_extractor(Extractor):
    __name = "udp_extractor"
    __width = 1.0 # 采样窗口长度
    __epsilon = 0.1 ** 3
    __leagal_dataFrame_st = pd.DataFrame()

    def __init__(self, leagal_dataFrame_st):
        print(f"### {self.__name}  Info: Initializing udp_extractor with the provided legal DataFrame with storage information.")
        # 初始化legal_dataFrame_st属性
        self.__width = 1.0
        self.__epsilon = 0.1 ** 3
        self.__leagal_dataFrame_st = pd.DataFrame()
        # 检查输入的legal_dataFrame_st是否为空
        if leagal_dataFrame_st.empty:
            print(f"!!! {self.__name} Error: The input legal_dataFrame_st is empty. Cannot initialize udp_extractor.")
            return
        self.__leagal_dataFrame_st = leagal_dataFrame_st

    def overview_tshark(self, udp_extractor_dir, merged_capFile_path, scene, id):
        udp_overview_csv_path = os.path.join(udp_extractor_dir, f"udp_overview.txt")
        overview_command = f"tshark -n -q -r {merged_capFile_path} -z conv,udp > {udp_overview_csv_path}"
        try:
            os.system(overview_command)
            # 检查导出目录中的目标文件是否为空，如果为空则说明提取失败，打印错误信息
            if not os.path.exists(udp_overview_csv_path) or os.path.getsize(udp_overview_csv_path) == 0:
                print(f"!!! {self.__name} extract Error: The UDP overview CSV file '{udp_overview_csv_path}' was not created successfully or is empty for record ID {id}. Please check the tshark command and the merged cap file. Skipping UDP feature extraction for this record.")
                return -1
            print(f"### {self.__name} extract Info: Successfully extracted UDP overview features for record ID {id} and exported to '{udp_overview_csv_path}'.")
        except Exception as e:
            print(f"!!! {self.__name} extract Error: An error occurred while extracting UDP overview features for record ID {id}. Error details: {e}. Skipping UDP feature extraction for this record.")
            return -1
        return 0

    """
    @brief IP对的判定，根据IP对的特征判断本机IP地址和服务器IP地址
    @param ipv4_src IPv4源地址
    @param ipv4_dst IPv4目的地址
    @param ipv6_src IPv6源地址
    @param ipv6_dst IPv6目的地址
    @param storage_Add 存储库地址信息
    @return (localIp,servIp,direction)，包含本机IP地址和服务器IP地址，如果无法判定则返回(None, None, None)
    """
    """
    # def ip_pair_judgement(self, ipv4_src, ipv4_dst, ipv6_src, ipv6_dst, storage_Add):
    #     local_ip = None
    #     serv_ip = None
    #     direction = None
    #     # ip协议的标准正则匹配
    #     ipv4_pattern = re.compile(
    #         r'^(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)'
    #         r'(?:\.(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$'
    #     )
    #     ipv6_pattern = re.compile(
    #         r'^([0-9a-fA-F]{1,4}:){1,7}[0-9a-fA-F]{1,4}$'
    #     )

    #     # 判定输入IP的合法性
    #     # 判定ipv4地址是否为字串且复合正则匹配
    #     if ipv4_src != "" and ipv4_dst != "":
    #         if not (ipv4_pattern.match(ipv4_src) and ipv4_pattern.match(ipv4_dst)):
    #             print(f"!!! {self.__name} ip_pair_judgement Warning: Invalid IPv4 addresses for judgement. IPv4 src: '{ipv4_src}', dst: '{ipv4_dst}'. Skipping IP pair judgement for this record.")
    #             return (None, None, None)
    #     # 判定ipv6地址是否为字串且复合正则匹配
    #     elif ipv6_src != "" and ipv6_dst != "":
    #         if not (ipv6_pattern.match(ipv6_src) and ipv6_pattern.match(ipv6_dst)):
    #             print(f"!!! {self.__name} ip_pair_judgement Warning: Invalid IPv6 addresses for judgement. IPv6 src: '{ipv6_src}', dst: '{ipv6_dst}'. Skipping IP pair judgement for this record.")
    #             return (None, None, None)
    #     else:
    #         print(f"!!! {self.__name} ip_pair_judgement Warning: Unable to determine IP protocol type or valid IP addresses for judgement. IPv4 src: '{ipv4_src}', dst: '{ipv4_dst}'; IPv6 src: '{ipv6_src}', dst: '{ipv6_dst}'. Skipping IP pair judgement for this record.")
    #         return (None, None, None)   

    #     # 首先获取本机IP地址信息文件localIP_info.txt中记录的本机IPv4地址和IPv6地址
    #     local_ipv4 = None
    #     local_ipv6 = None
    #     localIP_Info_path = os.path.join(storage_Add, "localIP_info.txt")
    #     if os.path.exists(localIP_Info_path):
    #         with open(localIP_Info_path, 'r') as f:
    #             # 逐行读取文件内容
    #             for line in f:
    #                 # 如果该行内容包含“IPv4”，则读取其中的IP地址信息
    #                 if "IPv4" in line:
    #                     # 匹配IPv4地址的正则表达式ipv4_pattern
    #                     ipv4_match = ipv4_pattern.search(line)
    #                     if ipv4_match:
    #                         local_ipv4 = ipv4_match.group()
    #                 # 如果该行内容包含“IPv6”，则读取其中的IP地址信息
    #                 elif "IPv6" in line:
    #                     # 匹配IPv6地址的正则表达式ipv6_pattern
    #                     ipv6_match = ipv6_pattern.search(line)
    #                     if ipv6_match:
    #                         local_ipv6 = ipv6_match.group()
    #     else:
    #         print(f"!!! {self.__name} ip_pair_judgement Warning: The local IP information file '{localIP_Info_path}' does not exist. Unable to retrieve local IP addresses for IP pair judgement.")

    #     # 判断IP协议类型
    #     # 如果IPv4地址不为空字符串以及NAN,则优先使用IPv4地址进行判定
    #     if not pd.isna(ipv4_src) and not pd.isna(ipv4_dst) and ipv4_src != "" and ipv4_dst != "":
    #         if ipv4_pattern.match(ipv4_src) and ipv4_pattern.match(ipv4_dst):
    #             # 寻找本机地址和服务端地址
    #             if ipv4_src.startswith("192.168.") or ipv4_src.startswith("10.") or ipv4_src.startswith("172.16."):
    #                 local_ip = ipv4_src
    #                 serv_ip = ipv4_dst
    #                 direction = "up"
    #             elif ipv4_dst.startswith("192.168.") or ipv4_dst.startswith("10.") or ipv4_dst.startswith("172.16."):
    #                 local_ip = ipv4_dst
    #                 serv_ip = ipv4_src
    #                 direction = "down"
    #     # 如果不存在IPv4地址但存在IPv6地址,则使用IPv6地址进行判定
    #     elif not pd.isna(ipv6_src) and not pd.isna(ipv6_dst) and ipv6_src != "" and ipv6_dst != "":
    #         # 再次确定ipv6地址满足正则匹配
    #         if ipv6_pattern.match(ipv6_src) and ipv6_pattern.match(ipv6_dst):
    #             # 寻找本机地址和服务端地址
    #             if local_ipv6 and (ipv6_src == local_ipv6 or ipv6_dst == local_ipv6):
    #                 local_ip = local_ipv6
    #                 serv_ip = ipv6_dst if ipv6_src == local_ipv6 else ipv6_src
    #                 direction = "up" if ipv6_src == local_ipv6 else "down"
    #             else:
    #                 # 如果无法通过本机IPv6地址进行判定，则直接将出现频率最高的IPv6地址作为本机地址
    #                 local_ip = ipv6_src if ipv6_src == local_ipv6 else ipv6_dst
    #                 serv_ip = ipv6_dst if local_ip == ipv6_src else ipv6_src
    #                 direction = None
    #                 print(f"!!! {self.__name} ip_pair_judgement Warning: Cannot find local IPv6 and Server IPv6")

    #     else:
    #         print(f"!!! {self.__name} ip_pair_judgement Warning: Unable to determine IP protocol type or valid IP addresses for judgement. IPv4 src: '{ipv4_src}', dst: '{ipv4_dst}'; IPv6 src: '{ipv6_src}', dst: '{ipv6_dst}'. Returning (None, None, None).")
    #         return (None, None, None)
    #     return (local_ip, serv_ip, direction)
    """

    """
    @brief IP对的判定，根据IP对的特征判断本机IP地址和服务器IP地址
    @param ipv4_src IPv4源地址
    @param ipv4_dst IPv4目的地址
    @param ipv6_src IPv6源地址
    @param ipv6_dst IPv6目的地址
    @param local_ipv4 本机IPv4地址
    @param local_ipv6 本机IPv6地址
    @return (localIp,servIp,direction)，包含本机IP地址和服务器IP地址，如果无法判定则返回(None, None, None)
    """
    def ip_pair_judgement(self, ipv4_src, ipv4_dst, ipv6_src, ipv6_dst, local_ipv4="", local_ipv6=""):
        local_ip = None
        serv_ip = None
        direction = None

        # 判定输入IP的合法性
        # 判定ipv4地址是否为字串且复合正则匹配
        if not pd.isna(ipv4_src) and not pd.isna(ipv4_dst) and ipv4_src != "" and ipv4_dst != "":
            if not (is_ipv4(ipv4_src) and is_ipv4(ipv4_dst)):
                print(f"!!! {self.__name} ip_pair_judgement Warning: Invalid IPv4 addresses for judgement. IPv4 src: '{ipv4_src}', dst: '{ipv4_dst}'. Skipping IP pair judgement for this record.")
                return (None, None, None)
        # 判定ipv6地址是否为字串且复合正则匹配
        elif not pd.isna(ipv6_src) and not pd.isna(ipv6_dst) and ipv6_src != "" and ipv6_dst != "":
            if not (is_ipv6(ipv6_src) and is_ipv6(ipv6_dst)):
                print(f"!!! {self.__name} ip_pair_judgement Warning: Invalid IPv6 addresses for judgement. IPv6 src: '{ipv6_src}', dst: '{ipv6_dst}'. Skipping IP pair judgement for this record.")
                return (None, None, None)
        else:
            print(f"!!! {self.__name} ip_pair_judgement Warning: Unable to determine IP protocol type or valid IP addresses for judgement. IPv4 src: '{ipv4_src}', dst: '{ipv4_dst}'; IPv6 src: '{ipv6_src}', dst: '{ipv6_dst}'. Skipping IP pair judgement for this record.")
            return (None, None, None)   

        # 判断IP协议类型
        # 如果IPv4地址不为空字符串,则优先使用IPv4地址进行判定
        if not pd.isna(ipv4_src) and not pd.isna(ipv4_dst) and ipv4_src != "" and ipv4_dst != "":
            try:
                if is_ipv4(ipv4_src) and is_ipv4(ipv4_dst):
                    # 寻找本机地址和服务端地址
                    if ipv4_src.startswith("192.168.") or ipv4_src.startswith("10.") or ipv4_src.startswith("172.16."):
                        local_ip = ipv4_src
                        serv_ip = ipv4_dst
                        direction = "up"
                    elif ipv4_dst.startswith("192.168.") or ipv4_dst.startswith("10.") or ipv4_dst.startswith("172.16."):
                        local_ip = ipv4_dst
                        serv_ip = ipv4_src
                        direction = "down"
            except Exception as e:
                print(f"!!! {self.__name} ip_pair_judgement Error: An error occurred while judging the IP pair for IPv4 addresses. IPv4 src: '{ipv4_src}', dst: '{ipv4_dst}'. Error details: {e}. Skipping IP pair judgement for this record.")
                return (None, None, None)
        # 如果不存在IPv4地址但存在IPv6地址,则使用IPv6地址进行判定
        elif not pd.isna(ipv6_src) and not pd.isna(ipv6_dst) and ipv6_src != "" and ipv6_dst != "":
            try:
                if is_ipv6(ipv6_src) and is_ipv6(ipv6_dst):
                    # 寻找本机地址和服务端地址
                    if local_ipv6 and (ipv6_src == local_ipv6 or ipv6_dst == local_ipv6):
                        local_ip = local_ipv6
                        serv_ip = ipv6_dst if ipv6_src == local_ipv6 else ipv6_src
                        direction = "up" if ipv6_src == local_ipv6 else "down"
                    else:
                        # 如果无法通过本机IPv6地址进行判定，则直接将出现频率最高的IPv6地址作为本机地址
                        local_ip = ipv6_src if ipv6_src == local_ipv6 else ipv6_dst
                        serv_ip = ipv6_dst if local_ip == ipv6_src else ipv6_src
                        direction = None
                        print(f"!!! {self.__name} ip_pair_judgement Warning: Cannot find local IPv6 and Server IPv6 in {ipv6_src} and {ipv6_dst} with local Ipv6 {local_ipv6}")
            except Exception as e:
                print(f"!!! {self.__name} ip_pair_judgement Error: An error occurred while judging the IP pair for IPv6 addresses. IPv6 src: '{ipv6_src}', dst: '{ipv6_dst}'. Error details: {e}. Skipping IP pair judgement for this record.")
                return (None, None, None)
        else:
            print(f"!!! {self.__name} ip_pair_judgement Warning: Unable to determine IP protocol type or valid IP addresses for judgement. IPv4 src: '{ipv4_src}', dst: '{ipv4_dst}'; IPv6 src: '{ipv6_src}', dst: '{ipv6_dst}'. Returning (None, None, None).")
            return (None, None, None)
        return (local_ip, serv_ip, direction)



    """
    @brief 获取cap文件中UDP协议的特征总览，并导出在目标文件路径下
    @param overview_file_path 总览信息的输出地址，包含文件名和扩展名
    @param merged_csvFiles_path 数据源csv文件的地址信息
    @param local_ipv4 本机IPv4地址
    @param local_ipv6 本机IPv6地址
    @return int值，overview_df数据结构，表示是否成功完成获取和导出操作，如果成功则返回overview数据,否则返回空DF
    """
    def overview(self, overview_file_path, merged_csvFiles_path, local_ipv4="", local_ipv6="") -> tuple:
        # 1. 文件路径的合法性检查
        # 检查文件路径是否为空或无效，如果无效则打印错误信息并返回-1
        if not overview_file_path or not isinstance(overview_file_path, str):
            print(f"!!! {self.__name} overview Error: The input overview_file_path is empty or invalid. Cannot perform overview extraction.")
            return -1, pd.DataFrame()
        # 检查csv文件是否存在，如果不存在则打印错误信息并返回-1
        if not merged_csvFiles_path or not os.path.exists(merged_csvFiles_path):
            print(f"!!! {self.__name} overview Error: The merged CSV file '{merged_csvFiles_path}' does not exist. Cannot perform overview extraction.")
            return -1, pd.DataFrame()
        # 2. 将csv文件读入为DF数据结构，并检查是否成功读取，如果读取失败则打印错误信息并返回-1
        try:
            merged_csv_df = pd.read_csv(merged_csvFiles_path)
            print(f"### {self.__name} overview Info: Successfully read the merged CSV file '{merged_csvFiles_path}' for overview extraction.")
        except Exception as e:
            print(f"!!! {self.__name} overview Error: An error occurred while reading the merged CSV file '{merged_csvFiles_path}'. Error details: {e}. Cannot perform overview extraction.")
            return -1, pd.DataFrame()
        if merged_csv_df.empty:
            print(f"!!! {self.__name} overview Error: The merged CSV file '{merged_csvFiles_path}' is empty. Cannot perform overview extraction.")
            return -1, pd.DataFrame()
        print(f"### {self.__name} overview Info: The merged CSV file '{merged_csvFiles_path}' has {len(merged_csv_df)} records and {len(merged_csv_df.columns)} columns for overview extraction.")
        # 3. 过滤获取udp协议的上下行信息
        # 检查frame.protocols列是否存在，如果不存在则打印错误信息并返回-1
        if 'frame.protocols' not in merged_csv_df.columns:
            print(f"!!! {self.__name} overview Error: The required column 'frame.protocols' is missing from the merged CSV file '{merged_csvFiles_path}'. Cannot perform overview extraction.")
            return -1, pd.DataFrame()
        # 过滤获取包含udp协议数据包的记录，如果没有则打印警告信息并返回-1
        udp_pattern = r'sll:ethertype:ip(v4|v6)?:udp:data'
        udp_df = merged_csv_df[merged_csv_df['frame.protocols'].str.contains(udp_pattern, case=False, na=False, regex=True)]
        if udp_df.empty:
            print(f"!!! {self.__name} overview Warning: No records containing 'sll:ethertype:ip:udp:data' protocol found in the merged CSV file '{merged_csvFiles_path}'. The resulting UDP overview will be empty.")
        else:
            print(f"### {self.__name} overview Info: Found {len(udp_df)} records containing 'udp:data' protocol in the merged CSV file '{merged_csvFiles_path}' for overview extraction.")
        # 4. 遍历udp_df中的记录，统计每个UDP会话的上下行数据包数量，并将统计结果导出在目标文件路径下
        # 创建统计缓冲区result_df，包含列：local_ip, local_port, serv_ip, serv_port, start_time, end_time, duration, up_packets, up_stream, down_packets, down_stream,total_packets, total_stream
        result_df = pd.DataFrame(columns=['local_ip', 'local_port', 'serv_ip', 'serv_port', 'start_time', 'end_time', 'duration', 'up_packets', 'up_stream', 'down_packets', 'down_stream', 'total_packets', 'total_stream'])
        result_list = []
        # 遍历udp_df中的记录，统计每个UDP会话的上下行数据包数量，并将统计结果添加到result_df中
        for index, row in udp_df.iterrows():
            # 获取该frame的ip地址
            ipv4_src = row["ip.src"] if 'ip.src' in row else ''
            ipv4_dst = row["ip.dst"] if 'ip.dst' in row else ''
            ipv6_src = row["ipv6.src"] if 'ipv6.src' in row else ''
            ipv6_dst = row["ipv6.dst"] if 'ipv6.dst' in row else ''
            src_port = row["udp.srcport"] if 'udp.srcport' in row else None
            dst_port = row["udp.dstport"] if 'udp.dstport' in row else None
            frame_time = row["frame.time_epoch"] if 'frame.time_epoch' in row else None
            frame_len = row["frame.len"] if 'frame.len' in row else None
            # 通过IP对的判定函数ip_pair_judgement来判断本机IP地址和服务器IP地址
            local_ip, serv_ip, direction = self.ip_pair_judgement(ipv4_src, ipv4_dst, ipv6_src, ipv6_dst, local_ipv4, local_ipv6)
            if local_ip is None or serv_ip is None or direction is None:
                print(f"!!! {self.__name} overview Warning: Unable to determine local IP, server IP, or direction for record at index {index}. Skipping this record for UDP overview statistics.")
                print(f"!!! {self.__name} overview Warning: This frame's IP information - IPv4 src: '{ipv4_src}', dst: '{ipv4_dst}'; IPv6 src: '{ipv6_src}', dst: '{ipv6_dst}', protocol: '{row.get('frame.protocols', None)}'.")
                print(f"!!! {self.__name} overview Warning: The output of ip_pair_judgement is local_ip:{local_ip}, serv_ip:{serv_ip} and direction:{direction}.")
                continue
            # 匹配判别local_port, serv_port
            if direction == "up":
                local_port = src_port
                serv_port = dst_port
            elif direction == "down":
                local_port = dst_port
                serv_port = src_port
            else:
                print(f"!!! {self.__name} overview Warning: Invalid direction '{direction}' determined for record at index {index}. Skipping this record for UDP overview statistics.")
                continue
            # 根据目前得到的local_ip, serv_ip, local_port, serv_port, frame_time, frame_len等信息来更新result_df中的统计结果
            session_filter = ((result_df['local_ip'] == local_ip) &
                              (result_df['serv_ip'] == serv_ip) &
                              (result_df['local_port'] == local_port) &
                              (result_df['serv_port'] == serv_port))
            if not result_df[session_filter].empty:
                # 如果result_df中已经存在该UDP会话的统计记录，则更新该记录的统计结果
                result_index = result_df[session_filter].index[0]
                if direction == "up":
                    result_df.at[result_index, 'up_packets'] += 1
                    result_df.at[result_index, 'up_stream'] += frame_len
                elif direction == "down":
                    result_df.at[result_index, 'down_packets'] += 1
                    result_df.at[result_index, 'down_stream'] += frame_len
                # 更新总数据包数量和总流量
                result_df.at[result_index, 'total_packets'] += 1
                result_df.at[result_index, 'total_stream'] += frame_len
                # 更新结束时间和持续时间
                if frame_time and (pd.isna(result_df.at[result_index, 'end_time']) or frame_time > result_df.at[result_index, 'end_time']):
                    result_df.at[result_index, 'end_time'] = frame_time
                    if pd.notna(result_df.at[result_index, 'start_time']):
                        result_df.at[result_index, 'duration'] = result_df.at[result_index, 'end_time'] - result_df.at[result_index, 'start_time']
            else:
                # 如果result_df中不存在该UDP会话的统计记录，则创建一条新的记录并添加到result_df中
                new_record = {
                    'local_ip': local_ip,
                    'local_port': local_port,
                    'serv_ip': serv_ip,
                    'serv_port': serv_port,
                    'start_time': frame_time,
                    'end_time': frame_time,
                    'duration': 0,
                    'up_packets': 1 if direction == "up" else 0,
                    'up_stream': frame_len if direction == "up" else 0,
                    'down_packets': 1 if direction == "down" else 0,
                    'down_stream': frame_len if direction == "down" else 0,
                    'total_packets': 1,
                    'total_stream': frame_len
                }
                result_df = pd.concat([result_df, pd.DataFrame([new_record])], ignore_index=True)
        # 将result_df导出在目标文件路径下，命名格式为udp_overview.txt
        try:
            # 将result_df按照total_stream列进行降序排序
            result_df.sort_values(by='total_stream', ascending=False, inplace=True)
            # 导出result_df到目标文件路径下
            result_df.to_csv(overview_file_path, index=False)
            # 检查导出目录中的目标文件是否为空，如果为空则说明导出失败，打印错误信息
            if not os.path.exists(overview_file_path) or os.path.getsize(overview_file_path) == 0:
                print(f"!!! {self.__name} overview Error: The UDP overview file '{overview_file_path}' was not created successfully or is empty for record ID {id}. Please check the result DataFrame and the export operation. Skipping UDP overview export for this record.")
                return -1, pd.DataFrame()
            print(f"### {self.__name} overview Info: Successfully exported UDP overview features to '{overview_file_path}' for record ID {id}.")
        except Exception as e:
            print(f"!!! {self.__name} overview Error: An error occurred while exporting the UDP overview file '{overview_file_path}' for record ID {id}. Error details: {e}. Skipping UDP overview export for this record.")
            return -1, pd.DataFrame()

        return 0, result_df

    """
    @brief  extract方法的实现，执行UDP协议的特征提取操作
    @param  legal_dataFrame_st 目标DF表格，包含合法的样本记录和存储库信息
    @return 整形值，表示是否成功完成提取操作
    @note   overview内容已经转移至overview_extractor中，旧有版本的extract方法实现暂时舍弃
    """
    """
    def extract(self, *args, **kwargs) -> any:
        # 1. 载入legal_dataFrame_st表格
        legal_dataFrame_st = self.__leagal_dataFrame_st

        # 2. 开始遍历legal_dataFrame_st中的各个样本记录，执行UDP协议的特征提取操作
        for index, row in legal_dataFrame_st.iterrows():
            # 3. 获取当前样本的scene,ID,storage_Add信息
            scene = row['scene']
            id = row['ID']
            storage_Add = row['storage_Add']
            # 如果获取的scene,id,strorage_Add为空或空字符串，打印警告信息并跳过该记录
            if pd.isna(scene) or pd.isna(id) or pd.isna(storage_Add) or scene == "" or id == "" or storage_Add == "":
                print(f"!!! {self.__name} extract Warning: Missing scene, ID, or storage_Add information for record at index {index}. Skipping this record.")
                continue
            print(f"### {self.__name} extract Info: Starting UDP feature extraction for record ID {id} in scene '{scene}' with storage address '{storage_Add}'.")
            # 4. 确保storage_Add目录下所需的信息存在：merged_csvfile,merged_capFiles
            merged_csvFile_dir = os.path.join(storage_Add, "merged_csvfiles")
            merged_csvFile_path = os.path.join(merged_csvFile_dir, f"merged_{scene}_{id}.csv")
            merged_capFile_dir = os.path.join(storage_Add, "merged_capFiles")
            merged_capFile_path = os.path.join(merged_capFile_dir, f"merged_{scene}_{id}.pcap")
            if not os.path.exists(merged_csvFile_path):
                print(f"!!! {self.__name} extract Warning: The merged CSV file '{merged_csvFile_path}' does not exist for record ID {id}. Skipping UDP feature extraction for this record.")
                continue
            if not os.path.exists(merged_capFile_path):
                print(f"!!! {self.__name} extract Warning: The merged cap file '{merged_capFile_path}' does not exist for record ID {id}. Skipping UDP feature extraction for this record.")
                continue
            # 5. 在storage_Add目录下创建udp_extractor目录
            udp_extractor_dir = os.path.join(storage_Add, "udp_extractor")
            if not os.path.exists(udp_extractor_dir):
                try:
                    os.makedirs(udp_extractor_dir, exist_ok=False)
                    print(f"### {self.__name} extract Info: Successfully created directory '{udp_extractor_dir}' for UDP feature extraction results of record ID {id}.")
                except FileExistsError:
                    print(f"!!! {self.__name} extract Error: The directory '{udp_extractor_dir}' already exists for record ID {id}. Please check the storage directory or remove the existing directory. Skipping UDP feature extraction for this record.")
                    continue
            # 6. 首先获取cap文件中UDP协议的特征总览，并导出在udp_extractor目录下，命名格式为udp_overview.txt
            # 首先查询本机IP地址信息文件localIP_info.txt中记录的本机IPv4地址和IPv6地址
            local_ipv4 = None
            local_ipv6 = None
            localIP_Info_path = os.path.join(storage_Add, "localIP_info.txt")
            if os.path.exists(localIP_Info_path):
                with open(localIP_Info_path, 'r') as f:
                    # 逐行读取文件内容
                    for line in f:
                        # 如果该行内容包含“IPv4”，则读取其中的IP地址信息
                        if "IPv4" in line:
                            # 匹配IPv4地址的正则表达式ipv4_pattern
                            for word in line.split():
                                clean_word = word.strip('.,:;')
                                if '.' in clean_word and clean_word.count('.') == 3 and is_ipv4(clean_word):
                                    local_ipv4 = clean_word
                        # 如果该行内容包含“IPv6”，则读取其中的IP地址信息
                        elif "IPv6" in line:
                            # 匹配IPv6地址的正则表达式ipv6_pattern
                            for word in line.split():
                                clean_word = word.strip('.,:;')
                                if ':' in clean_word and is_ipv6(clean_word):
                                    local_ipv6 = clean_word
            else:
                print(f"!!! {self.__name} ip_pair_judgement Warning: The local IP information file '{localIP_Info_path}' does not exist. Unable to retrieve local IP addresses for IP pair judgement.")
                print(f"!!! {self.__name} Error: YOU NEED TO RUN localIP_extractor FIRST TO GET THE LOCAL IP ADDRESSES FOR UDP OVERVIEW FEATURE EXTRACTION. Skipping UDP overview feature extraction for record ID {id}.")
                continue
            # 完成本机IP搜索后，开始统计UDP数据总览特征，并导出在目标文件路径下
            udp_overview_csv_path = os.path.join(udp_extractor_dir, f"udp_overview.txt")

            # 中断测试
            # input(f"pause!!! check local_ipv4: {local_ipv4}, and local_ipv6: {local_ipv6}")
            
            overview_result,overview_df = self.overview(udp_overview_csv_path, merged_csvFile_path, local_ipv4, local_ipv6)
            if overview_result != 0:
                print(f"!!! {self.__name} extract Warning: There were issues during the extraction of UDP overview features for record ID {id}. Please check the logs for details.")
            else:
                print(f"### {self.__name} extract Info: Successfully completed the extraction of UDP overview features for record ID {id}.")
            

        return 0
    """

    
    def extract(self, *args, **kwargs):
        result = 0
        # 1. 遍历legal_dataFrame_st下的各个样本
        for index, row in self.__leagal_dataFrame_st.iterrows():
            # 获取样本信息
            id = row["ID"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]

            # 2. 检查UDP特征提取所需的文件是否存在
            # 2.1 检查源文件merged_capfiles文件是否存在
            merCapFiles_dir = os.path.join(storage_Add, "merged_capFiles")
            merCapFiles_path = os.path.join(merCapFiles_dir, f"merged_{scene}_{id}.pcap")
            if not os.path.exists(merCapFiles_dir):
                print(f"!!! {self.__name} - extract Error: There is no effective directory for merged_capFiles in sample: {scene}-{id}!")
                result -= 1
                continue
            if not os.path.exists(merCapFiles_path):
                print(f"!!! {self.__name} - extract Error: There is no merged_capFiles in sample: {scene}-{id}!")
                result -= 1
                continue
            # 2.2 检查源文件merged_csvfiles文件是否存在
            merCsvFiles_dir = os.path.join(storage_Add, "merged_csvFiles")
            merCsvFiles_path = os.path.join(merCsvFiles_dir, f"merged_{scene}_{id}.csv")
            if not os.path.exists(merCsvFiles_dir):
                print(f"!!! {self.__name} - extract Error: There is no effective directory for merged_csvFiles in sample: {scene}-{id}!")
                result -= 1
                continue
            if not os.path.exists(merCsvFiles_path):
                print(f"!!! {self.__name} - extract Error: There is no merged_csvFiles in sample: {scene}-{id}!")
                result -= 1
                continue
            # 2.3 检查总览文件overview_files文件是否存在
            overviewFile_dir = os.path.join(storage_Add, "overview_extractor")
            overviewFile_path = os.path.join(overviewFile_dir, "udp_overview.txt")
            if not os.path.exists(overviewFile_dir):
                print(f"!!! {self.__name} - extract Error: There is no effective directory for overview files in sample: {scene}-{id}!")
                result -= 1
                continue
            if not os.path.exists(overviewFile_path):
                print(f"!!! {self.__name} - extract Error: There is no overview file in sample: {scene}-{id}!")
                result -= 1
                continue
            # 2.4 检查本机地址localIP_files文件是否存在
            localIP_dir = os.path.join(storage_Add, "localIP_extractor")
            localIP_protocol = [
                "ipv4",
                "ipv6",
            ]
            localIP_path = []
            for protocol in localIP_protocol:
                localIP_path.append(
                    os.path.join(localIP_dir, f"{protocol}.txt")
                )
            if len(localIP_path) == 0:
                print(f"!!! {self.__name} - extractor Error: no IP files found in sample: {scene}-{id}!")
                result -= 1
                continue
            for path in localIP_path:
                if not os.path.exists(path):
                    print(f"!!! {self.__name} - extractor Error: Cannot find file: {os.path.basename(path)} in sample: {scene}-{id}!")
                    result -= 1
                    continue
            # 完成所需文件的检查
            print(f"### {self.__name} - extractor Info: All files needed for udp extraction is already!")

            # input("检查上下行原文件是否存在且合法")

            # 3. 获取需要观测的目标流通道
            # 读入overview_files文件
            # target_flow存储目标流通的信息，字典列表结构
            # 存储信息：local_ip, serv_ip, down_load, up_load, total_load
            target_flow = []
            try:
                with open(overviewFile_path, "r") as f:
                    # 创建负载缓存-字典列表
                    flow_item_list = []
                    # 逐行读取信息并进行负载统计
                    lines = f.readlines()
                    for line in lines:
                        if "<->" in line:
                            # 从行字串中获取相关信息
                            local_ip, serv_ip = get_IPpair_from_line(line)
                            down_load, up_load, total_load = get_load_from_line(line)
                            # 建立字典
                            flow_item = {
                                "local_ip":     local_ip,
                                "serv_ip":      serv_ip,
                                "down_load":    down_load,
                                "up_load":      up_load,
                                "total_load":   total_load
                            }
                            flow_item_list.append(flow_item)
                    # 统计负载占比，获取目标流通道
                    load_sum = sum([int(item["total_load"]) for item in flow_item_list])
                    cache = 0
                    for item in flow_item_list:
                        cache += item["total_load"]
                        target_flow.append(item)
                        if (cache/load_sum >= 0.99):
                            break
            except Exception as e:
                print(f"!!! {self.__name} - extractor Error: While read overview file: {overviewFile_path}, error: {e}")
                result -= 0
                continue
            print(f"### {self.__name} - extractor Info: Find target Flow: {target_flow}.")

            # input("检查提取器是否成功获取 目标流通道")

            # 4. 读入merge_capfiles文件，过滤出UDP数据包
            # 读入merge_capfile
            merged_csv_df = pd.DataFrame()
            try:
                merged_csv_df = pd.read_csv(merCsvFiles_path)
                if merged_csv_df.empty:
                    print(f"!!! {self.__name} - extractor Error: Didnot find merged csvfiles or csvfiles is empty")
                    result -= 1
                    continue
            except Exception as e:
                print(f"!!! {self.__name} - extractor Error: In merged_capfiles Reading, there is errors: {e}")
            # 对merge_capfiles进行udp过滤
            udp_pattern = r'sll:ethertype:ip(v4|v6)?:udp:data'
            udp_df = merged_csv_df[merged_csv_df['frame.protocols'].str.contains(udp_pattern, case=False, na=False, regex=True)]

            # input(f"检查udp数据包过滤器，过滤所得的长度是{len(udp_df)}，包含列明：{udp_df.columns}")
            
            # 5. 以target_flow作为过滤条件，过滤提取各流通到的上下行信息，并且汇总生成一个总应用通道的上下行信息
            up_df = pd.DataFrame() # 缓存，临时缓存处理中得到的上行信息
            down_df = pd.DataFrame() # 缓存，临时缓存处理中得到的下行信息
            up_total_df = pd.DataFrame() # 缓存，临时缓存总通道统计的上行信息
            down_total_df = pd.DataFrame() # 缓存，临时缓存总通道统计的下行信息
            # 遍历target_flow当中的各个流通到，提取上下行信息
            for flow in target_flow:
                # 从udp_df总信息框中过滤出目标flow的上下行流信息
                up_df = udp_df[ ((udp_df["frame.protocols"]=="sll:ethertype:ip:udp:data") 
                                    & (udp_df["ip.src"]==flow["local_ip"]) 
                                    & (udp_df["ip.dst"]==flow["serv_ip"])) 
                                | ((udp_df["frame.protocols"]=="sll:ethertype:ipv6:udp:data") 
                                    & (udp_df["ipv6.src"]==flow["local_ip"]) 
                                    & (udp_df["ipv6.dst"]==flow["serv_ip"]))].copy()
                down_df = udp_df[ ((udp_df["frame.protocols"]=="sll:ethertype:ip:udp:data") 
                                    & (udp_df["ip.src"]==flow["serv_ip"]) 
                                    & (udp_df["ip.dst"]==flow["local_ip"])) 
                                | ((udp_df["frame.protocols"]=="sll:ethertype:ipv6:udp:data") 
                                    & (udp_df["ipv6.src"]==flow["serv_ip"]) 
                                    & (udp_df["ipv6.dst"]==flow["local_ip"]))].copy()
                if up_df.empty or down_df.empty:
                    print(f"!!! {self.__name} - extractor Warning: When extract flow:{flow["local_ip"]}-{flow["serv_ip"]}, the filter output from udp_df is empty")
                    flow["up_df"] = up_df
                    flow["down_df"] = down_df
                    result -= 1
                    continue
                print(f"### {self.__name} - extractor Info: Got flow:{flow["local_ip"]}-{flow["serv_ip"]} from udp_df, up-size:{up_df.size}, down-size:{down_df.size}")
                # 将上下行信息作为数据内容，写入target_flow字典中
                flow["up_df"] = up_df
                flow["down_df"] = down_df
                # 汇总各通道的上下行信息
                up_total_df = pd.concat([up_total_df, up_df], ignore_index=True)
                down_total_df = pd.concat([down_total_df, down_df], ignore_index=True)
            
            # 输出内容的合法性检测与总通道的时间排序
            if up_total_df.empty:
                print(f"!!! {self.__name} - extractor Error: up_total_df is empty, something Wrong Happened.")
                resutl -= 1
                continue
            print(f"### {self.__name} - extractor Info: Got up_total_df as: size({up_total_df.size})")
            if down_total_df.empty:
                print(f"!!! {self.__name} - extractor Error: down_total_df is empty, something Wrong Happened.")
                resutl -= 1
                continue
            print(f"### {self.__name} - extractor Info: Got down_total_df as: size({down_total_df.size})")
            up_total_df = up_total_df.sort_values(by="frame.time_epoch", ascending=True)
            down_total_df = down_total_df.sort_values(by="frame.time_epoch", ascending=True)
            # 根据遍历结果汇总生成一个应用总通道的上下行统计
            total_item = {
                "local_ip":     "all",
                "serv_ip":      "all",
                "down_load":    sum(flow["down_load"] for flow in target_flow if "down_load" in flow),
                "up_load":      sum(flow["up_load"] for flow in target_flow if "up_load" in flow),
                "total_load":   sum(flow["total_load"] for flow in target_flow if "total_load" in flow),
                "up_df":        up_total_df.copy(),
                "down_df":      down_total_df.copy()
            }
            # 将总通道的上下行信息写入到target_flow当中
            target_flow.append(total_item)
            
            # input(f"检查数据特征提取是否完备：{len(target_flow[0])}")

            # 6. 对target_flow中的各个流，以及应用总通道的上下行数据特征，进行统计
            features = [] # 缓存，缓存采样过程中所有窗口的数据特征
            up_df = pd.DataFrame() # 缓存，暂时缓存上行流信息
            down_df = pd.DataFrame() # 缓存，暂时缓存下行流信息
            for flow in target_flow:
                # 6.1 由于特征分为上下行特征，我们首先从udp_df总信息框中过滤出目标flow的上下行流信息
                up_df = flow["up_df"]
                down_df = flow["down_df"]
                if up_df.empty or down_df.empty:
                    print(f"### {self.__name} - extractor Warning: When extract flow:{flow["local_ip"]}-{flow["serv_ip"]}, the filter output from udp_df is empty")
                    flow["udp_features"] = pd.DataFrame()
                    result -= 1
                    continue
                print(f"### {self.__name} - extractor Info: Got flow:{flow["local_ip"]}-{flow["serv_ip"]} from udp_df, up-length:{len(up_df)}, down-length:{len(down_df)}")
                
                # 6.2 获取整个通信过程中的起止时间
                # 由于原始merged_csvfiles中的时间戳整数部分单位为秒级，所以我们限定起止时间均为秒级
                start_time = int(min(up_df["frame.time_epoch"].min(), down_df["frame.time_epoch"].min()))
                end_time = int(max(up_df["frame.time_epoch"].max(), down_df["frame.time_epoch"].max())) + 1 # 整形将会向下取一次，我们需要补充被省略的内容
                
                # 6.3 计算包抵达时间间隔
                # 创建缓存
                pre_time = up_df.loc[up_df.index[0], "frame.time_epoch"]
                cur_time = 0
                delays = []
                # 上行包抵达间隔的统计
                for index, row in up_df.iterrows():
                    # 获取当前上行包的时间
                    cur_time = row["frame.time_epoch"]
                    # 计算当前上行包时间的前向差分，获取上行包间隔
                    delay = cur_time - pre_time
                    # 更新上行包时间戳
                    pre_time = cur_time
                    # 添加进包间隔列表
                    delays.append(delay)
                up_df["frame_interval"] = delays
                # 缓存复位
                pre_time = down_df.loc[down_df.index[0], "frame.time_epoch"]
                cur_time = 0
                delays = []
                # 下行包抵达间隔的统计
                for index, row in down_df.iterrows():
                    # 获取当前上行包的时间
                    cur_time = row["frame.time_epoch"]
                    # 计算当前上行包时间的前向差分，获取上行包间隔
                    delay = cur_time - pre_time
                    # 更新上行包时间戳
                    pre_time = cur_time
                    # 添加进包间隔列表
                    delays.append(delay)
                down_df["frame_interval"] = delays
                
                # 6.4 对上下行流信息统计UDP数据特征
                # 创建必要的变量
                features = [] # 列表，存储各个采样窗口下数据特征的字典信息，最终将会以字典列表的形式构建DF数据结构
                up_tailTime_of_last_second = up_df.loc[up_df.index[0], 'frame.time_epoch']          # 获取上行流数据中第一个包的时间戳
                down_tailTime_of_last_second = down_df.loc[down_df.index[0], 'frame.time_epoch']    # 获取下行流数据中第一个包的时间戳
                for startTime_of_curWindow in np.arange(start_time, end_time, self.__width):
                    # 限定当前采样窗口的前后边界
                    endTime_of_curWindow = startTime_of_curWindow + self.__width
                    # 将Unix时间戳标准下的startTime_of_curWindow转换为北京时间，便于后续比较
                    curTime_of_UTC8 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(startTime_of_curWindow))
                    # 根据采样窗口的前后边界，截取窗口内的上下行信息
                    upDF_in_curWindow = up_df[(up_df['frame.time_epoch'] >= startTime_of_curWindow) &
                                                (up_df['frame.time_epoch'] < endTime_of_curWindow)]
                    downDF_in_curWindow = down_df[(down_df['frame.time_epoch'] >= startTime_of_curWindow) &
                                                (down_df['frame.time_epoch'] < endTime_of_curWindow)]

                    #获取各项特征
                    # 包数量基础上的统计特征
                    # 当前采样窗口内的上下行包数量
                    up_numPackets = len(upDF_in_curWindow)
                    down_numPackets = len(downDF_in_curWindow)

                    # 包数量突变率统计
                    up_mutation_numPackets = 0
                    down_mutation_numPackets = 0
                    if len(features) == 0:
                        up_mutation_numPackets = 0
                        down_mutation_numPackets = 0
                    else:
                        up_pre_numPackets = features[-1]["up_numPackets"]
                        down_pre_numPackets = features[-1]["down_numPackets"]
                        # up_mutation_numPackets = (up_numPackets - up_pre_numPackets) / (up_pre_numPackets + self.__epsilon)
                        # down_mutation_numPackets = (down_numPackets - down_pre_numPackets) / (down_pre_numPackets + self.__epsilon)
                        up_mutation_numPackets = (up_numPackets - up_pre_numPackets) / up_pre_numPackets if up_pre_numPackets != 0 else 0
                        down_mutation_numPackets = (down_numPackets - down_pre_numPackets) / down_pre_numPackets if down_pre_numPackets != 0 else 0

                    # 包长度基础上的统计特征
                    # 当前窗口内上下行包长度的均值
                    up_mean_lenPackets = upDF_in_curWindow['frame.len'].mean() if not upDF_in_curWindow.empty else 0
                    down_mean_lenPackets = downDF_in_curWindow['frame.len'].mean() if not downDF_in_curWindow.empty else 0
                    # 当前窗口内上下行的流量
                    up_dataStream_lenPackets = upDF_in_curWindow['frame.len'].sum()
                    down_dataStream_lenPackets = downDF_in_curWindow['frame.len'].sum()

                    # 包抵达时间戳基础上的统计特征
                    # 包间隔构建的均值、最值、标准差、CV系数
                    # 上行数据
                    if not upDF_in_curWindow.empty:
                        # 更新均值、最值、标准差
                        up_mean_intPackets = upDF_in_curWindow["frame_interval"].mean()
                        up_min_intPackets = upDF_in_curWindow["frame_interval"].min()
                        up_max_intPackets = upDF_in_curWindow["frame_interval"].max()
                        up_std_intPackets = upDF_in_curWindow["frame_interval"].std() if not (upDF_in_curWindow.index.size == 1) else 0
                        up_cv_intPacktes = up_std_intPackets / (up_mean_intPackets + self.__epsilon)

                        # 更新当前窗口中最后一个数据包时间戳，为下一个窗口统计包间隔提供参考
                        up_tailTime_of_last_second = upDF_in_curWindow.iloc[-1]["frame.time_epoch"]
                    else:
                        up_mean_intPackets = startTime_of_curWindow + 1 - up_tailTime_of_last_second
                        up_min_intPackets = startTime_of_curWindow + 1 - up_tailTime_of_last_second
                        up_max_intPackets = startTime_of_curWindow + 1 - up_tailTime_of_last_second
                        up_std_intPackets = 0
                        up_cv_intPacktes = 0
                    # 下行数据
                    if not downDF_in_curWindow.empty:
                        # 更新均值、最值、标准差
                        down_mean_intPackets = downDF_in_curWindow["frame_interval"].mean()
                        down_min_intPackets = downDF_in_curWindow["frame_interval"].min()
                        down_max_intPackets = downDF_in_curWindow["frame_interval"].max()
                        down_std_intPackets = downDF_in_curWindow["frame_interval"].std() if not (upDF_in_curWindow.index.size == 1) else 0
                        down_cv_intPacktes = down_std_intPackets / (down_mean_intPackets + self.__epsilon)

                        # 更新当前窗口中最后一个数据包时间戳，为下一个窗口统计包间隔提供参考
                        down_tailTime_of_last_second = downDF_in_curWindow.iloc[-1]["frame.time_epoch"]
                    else:
                        down_mean_intPackets = startTime_of_curWindow + 1 - down_tailTime_of_last_second
                        down_min_intPackets = startTime_of_curWindow + 1 - down_tailTime_of_last_second
                        down_max_intPackets = startTime_of_curWindow + 1 - down_tailTime_of_last_second
                        down_std_intPackets = 0
                        down_cv_intPacktes = 0

                    # 将3个方向的数据特征汇总合并成为一个字典结构
                    features_map = {
                        "startTime_of_curWin_Unix": startTime_of_curWindow,
                        "startTime_of_curWin_UTC8": curTime_of_UTC8,
                        
                        "up_numPackets": up_numPackets,
                        "up_mutation_numPackets": up_mutation_numPackets,
                        "down_numPackets": down_numPackets,
                        "down_mutation_numPackets": down_mutation_numPackets,
                    
                        "up_mean_lenPackets": up_mean_lenPackets,
                        "up_datastream_lenPackets": up_dataStream_lenPackets,
                        "down_mean_lenPackets": down_mean_lenPackets,
                        "down_datastream_lenPackets": down_dataStream_lenPackets,

                        "up_mean_intPackets": up_mean_intPackets,
                        "up_min_intPackets": up_min_intPackets,
                        "up_max_intPackets": up_max_intPackets,
                        "up_std_intPackets": up_std_intPackets,
                        "up_cv_intPacktes": up_cv_intPacktes,
                        "down_mean_intPackets": down_mean_intPackets,
                        "down_min_intPackets": down_min_intPackets,
                        "down_max_intPackets": down_max_intPackets,
                        "down_std_intPackets": down_std_intPackets,
                        "down_cv_intPacktes": down_cv_intPacktes,
                    }
                    features.append(features_map)
                # 将该流通道下的特征字典列表转换为DF数据结构
                features_df = pd.DataFrame(features)
                # 将统计矩阵存储在该流通道的缓存中
                flow["udp_features"] = features_df
            # 将target_flow当中的数据内容，按照负载大小的降序，重新排序
            target_flow.sort(key = lambda x: x["total_load"], reverse=True)

            # input(f"检查数据特征提取是否完备：{len(target_flow[0])}")

            # 7. 根据指令信息决议是否导出数据矩阵
            # 首先在storage_Add目录下创建udp_extractor目录，用于缓存各个流通道的数据
            output_dir = os.path.join(storage_Add, "udp_extractor")
            try:
                os.makedirs(output_dir, exist_ok=False)
                print(f"### {self.__name} - extractor Info: Succesfully making udp_extractor opt-dir in: {output_dir}")
            except Exception as e:
                print(f"!!! {self.__name} - extractor Error: something wrong in making udp_extractor opt-dir with e: {e}!")
                result -= 1
                continue
            # 创建udp流通道数据的总览文件，向其中写入各流通道的总体数据情况
            udp_overview_file = os.path.join(output_dir, ".txt")
            try:
                with open(udp_overview_file, "w", encoding="utf-8") as f:
                    f.write("target flows under UDP-Extractor:\n")
                    for flow in target_flow:
                        f.write(
                            f"local_ip:{flow["local_ip"]}," + "\t"
                            + f"serv_ip:{flow["serv_ip"]}," + "\t"
                            + f"down_load:{flow["down_load"]}," + "\t"
                            + f"up_load:{flow["up_load"]} Bytes," + "\t"
                            + f"total_load:{flow["down_load"]} Bytes," + "\t"
                        )
            except Exception as e:
                print(f"!!! {self.__name} - extractor Info: something wrong in generating udp-overview- file with e: {e}!")
                result -= 1
                continue
            # 遍历各个目标流通道，将UDP数据特征持久化到样本存储目录下
            for i, flow in enumerate(target_flow):
                sample_dir = os.path.join(output_dir, f"{scene}_{id}_flow{i}")
                try:
                    os.makedirs(sample_dir, exist_ok=False)
                    print(f"### {self.__name} - extracotr Info: Successfully making ourput-dir for sample:{scene}-{id}, {flow["local_ip"]}<->{flow["serv_ip"]}.")
                    # 在样本下的对应通道输出目录中，输出各个数据矩阵
                    flow["up_df"].to_csv(os.path.join(sample_dir, "up.csv"), index=False)
                    flow["down_df"].to_csv(os.path.join(sample_dir, "down.csv"), index=False)
                    flow["udp_features"].to_csv(os.path.join(sample_dir, "udp_features.csv"), index=False)
                except Exception as e:
                    print(f"!!! {self.__name} - extracotr Error: Something wrong in sample:{scene}-{id}, {flow["local_ip"]}<->{flow["serv_ip"]}'s UDP FEATURES OUTPUTING!\n"
                        + f"the ERROR is: {e}!\n")
                    result -= 1
                    continue
            # 完成该样本的UDP数据特征抓取
        # 完成所有样本的UDP数据特征抓取
        return result

    def toString(self) -> str:
        return self.__name



if __name__ == "__main__":
    print("!!!Run udp_extractor.py test")