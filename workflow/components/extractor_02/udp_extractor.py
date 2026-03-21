import os
import re
import sys
import pandas as pd
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

"""
@brief 类udp_extractor，继承自Extractor虚拟父类
"""
class udp_extractor(Extractor):
    __name = "udp_extractor"
    __leagal_dataFrame_st = pd.DataFrame()

    def __init__(self, leagal_dataFrame_st):
        print(f"### {self.__name}  Info: Initializing udp_extractor with the provided legal DataFrame with storage information.")
        # 初始化legal_dataFrame_st属性
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

    # """
    # @brief IP对的判定，根据IP对的特征判断本机IP地址和服务器IP地址
    # @param ipv4_src IPv4源地址
    # @param ipv4_dst IPv4目的地址
    # @param ipv6_src IPv6源地址
    # @param ipv6_dst IPv6目的地址
    # @param storage_Add 存储库地址信息
    # @return (localIp,servIp,direction)，包含本机IP地址和服务器IP地址，如果无法判定则返回(None, None, None)
    # """
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
    # def extract(self, *args, **kwargs) -> any:
    #     # 1. 载入legal_dataFrame_st表格
    #     legal_dataFrame_st = self.__leagal_dataFrame_st

    #     # 2. 开始遍历legal_dataFrame_st中的各个样本记录，执行UDP协议的特征提取操作
    #     for index, row in legal_dataFrame_st.iterrows():
    #         # 3. 获取当前样本的scene,ID,storage_Add信息
    #         scene = row['scene']
    #         id = row['ID']
    #         storage_Add = row['storage_Add']
    #         # 如果获取的scene,id,strorage_Add为空或空字符串，打印警告信息并跳过该记录
    #         if pd.isna(scene) or pd.isna(id) or pd.isna(storage_Add) or scene == "" or id == "" or storage_Add == "":
    #             print(f"!!! {self.__name} extract Warning: Missing scene, ID, or storage_Add information for record at index {index}. Skipping this record.")
    #             continue
    #         print(f"### {self.__name} extract Info: Starting UDP feature extraction for record ID {id} in scene '{scene}' with storage address '{storage_Add}'.")
    #         # 4. 确保storage_Add目录下所需的信息存在：merged_csvfile,merged_capFiles
    #         merged_csvFile_dir = os.path.join(storage_Add, "merged_csvfiles")
    #         merged_csvFile_path = os.path.join(merged_csvFile_dir, f"merged_{scene}_{id}.csv")
    #         merged_capFile_dir = os.path.join(storage_Add, "merged_capFiles")
    #         merged_capFile_path = os.path.join(merged_capFile_dir, f"merged_{scene}_{id}.pcap")
    #         if not os.path.exists(merged_csvFile_path):
    #             print(f"!!! {self.__name} extract Warning: The merged CSV file '{merged_csvFile_path}' does not exist for record ID {id}. Skipping UDP feature extraction for this record.")
    #             continue
    #         if not os.path.exists(merged_capFile_path):
    #             print(f"!!! {self.__name} extract Warning: The merged cap file '{merged_capFile_path}' does not exist for record ID {id}. Skipping UDP feature extraction for this record.")
    #             continue
    #         # 5. 在storage_Add目录下创建udp_extractor目录
    #         udp_extractor_dir = os.path.join(storage_Add, "udp_extractor")
    #         if not os.path.exists(udp_extractor_dir):
    #             try:
    #                 os.makedirs(udp_extractor_dir, exist_ok=False)
    #                 print(f"### {self.__name} extract Info: Successfully created directory '{udp_extractor_dir}' for UDP feature extraction results of record ID {id}.")
    #             except FileExistsError:
    #                 print(f"!!! {self.__name} extract Error: The directory '{udp_extractor_dir}' already exists for record ID {id}. Please check the storage directory or remove the existing directory. Skipping UDP feature extraction for this record.")
    #                 continue
    #         # 6. 首先获取cap文件中UDP协议的特征总览，并导出在udp_extractor目录下，命名格式为udp_overview.txt
    #         # 首先查询本机IP地址信息文件localIP_info.txt中记录的本机IPv4地址和IPv6地址
    #         local_ipv4 = None
    #         local_ipv6 = None
    #         localIP_Info_path = os.path.join(storage_Add, "localIP_info.txt")
    #         if os.path.exists(localIP_Info_path):
    #             with open(localIP_Info_path, 'r') as f:
    #                 # 逐行读取文件内容
    #                 for line in f:
    #                     # 如果该行内容包含“IPv4”，则读取其中的IP地址信息
    #                     if "IPv4" in line:
    #                         # 匹配IPv4地址的正则表达式ipv4_pattern
    #                         for word in line.split():
    #                             clean_word = word.strip('.,:;')
    #                             if '.' in clean_word and clean_word.count('.') == 3 and is_ipv4(clean_word):
    #                                 local_ipv4 = clean_word
    #                     # 如果该行内容包含“IPv6”，则读取其中的IP地址信息
    #                     elif "IPv6" in line:
    #                         # 匹配IPv6地址的正则表达式ipv6_pattern
    #                         for word in line.split():
    #                             clean_word = word.strip('.,:;')
    #                             if ':' in clean_word and is_ipv6(clean_word):
    #                                 local_ipv6 = clean_word
    #         else:
    #             print(f"!!! {self.__name} ip_pair_judgement Warning: The local IP information file '{localIP_Info_path}' does not exist. Unable to retrieve local IP addresses for IP pair judgement.")
    #             print(f"!!! {self.__name} Error: YOU NEED TO RUN localIP_extractor FIRST TO GET THE LOCAL IP ADDRESSES FOR UDP OVERVIEW FEATURE EXTRACTION. Skipping UDP overview feature extraction for record ID {id}.")
    #             continue
    #         # 完成本机IP搜索后，开始统计UDP数据总览特征，并导出在目标文件路径下
    #         udp_overview_csv_path = os.path.join(udp_extractor_dir, f"udp_overview.txt")

    #         # 中断测试
    #         # input(f"pause!!! check local_ipv4: {local_ipv4}, and local_ipv6: {local_ipv6}")
            
    #         overview_result,overview_df = self.overview(udp_overview_csv_path, merged_csvFile_path, local_ipv4, local_ipv6)
    #         if overview_result != 0:
    #             print(f"!!! {self.__name} extract Warning: There were issues during the extraction of UDP overview features for record ID {id}. Please check the logs for details.")
    #         else:
    #             print(f"### {self.__name} extract Info: Successfully completed the extraction of UDP overview features for record ID {id}.")
            

    #     return 0
    
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
            merCsvFiles_path = os.path.join(merCapFiles_dir, f"merged_{scene}_{id}.csv")
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
            overviewFile_path = os.path.join(storage_Add, "udp_overview.txt")
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
                "ipv4"
                "ipv6"
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

            # 3. 获取需要观测的目标流通道
            # 读入overview_files文件
            target_flow = []
            try:
                with open(overviewFile_path, "r") as f:
                    # 创建负载缓存-字典列表
                    flow_item_list = []
                    # 逐行读取信息并进行负载统计
                    lines = f.readlines()
                    for line in lines:
                        if "<->" in line:
                            parts = line.strip().split()
                            # 读入IP对
                            local_ip = None
                            serv_ip = None
                            # 读入负载
                            load = None
                            # 读入起始截至时间
                            start_time = None
                            end_time = None
                            # 建立字典
                            flow_item = {
                                "local_ip": local_ip,
                                "serv_ip": serv_ip,
                                "load": load,
                                "start_time": start_time,
                                "end_time": end_time
                            }
                            flow_item_list.append(flow_item)
                    # 统计负载占比，获取目标流通道
                    load_sum = sum([int(item["load"]) for item in flow_item_list])
                    for item in flow_item_list:
                        if (判定负载为目标的条件):
                            target_flow.append(item)
            except Exception as e:
                print(f"!!! {self.__name} - extractor Error: While read overview file: {overviewFile_path}, error: {e}")
                result -= 0
                continue

            # 4. 在merge_capfiles中对各个流通道进行统计

            # 5. 根据指令信息决议是否绘制可视化结果

            # 6. 根据指令信息决议是否导出数据矩阵

        return result

    def toString(self) -> str:
        return self.__name



if __name__ == "__main__":
    print("!!!Run udp_extractor.py test")