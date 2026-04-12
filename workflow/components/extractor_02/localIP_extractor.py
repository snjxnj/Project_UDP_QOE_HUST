import os
import re
import sys
import pandas as pd
import datetime as dt
from ipaddress import ip_address, IPv4Address, IPv6Address
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
@brief 类localIP_extractor，继承自Extractor虚拟父类，目标是搜索当前样本下的本机IP地址
"""
class localIP_Extractor(Extractor):
    __name = "localIP_Extractor"
    __leagal_dataFrame_st = pd.DataFrame()
    __state = "not_found"
    
    def __init__(self, leagal_dataFrame_st):
        print(f"### {self.__name} Info: Initializing {self.__name} with the provided legal DataFrame with storage information.")
        # 检查输入的legal_dataFrame_st是否为空
        if leagal_dataFrame_st.empty:
            self.__state = "empty_legal_dataFrame_st"
            print(f"!!! {self.__name} Error: The input legal_dataFrame_st is empty. Cannot initialize {self.__name}.")
            return
        # 初始化legal_dataFrame_st属性
        self.__leagal_dataFrame_st = leagal_dataFrame_st
    
    """
    @brief  初期版本的本机IP搜索，通过分析上下负载中的IP占比，默认占比最高的为本机IP
    @return int值，标明是否完成
    @note   当前该版本会受到本机IPv6地址变换的影响，因而无法稳定投入使用
    """
    def extract_V1(self):
        result = -1
        # 1. 遍历legal_dataFrame_st中的各个样本记录，搜索当前样本下的本机IP地址
        for index, row in self.__leagal_dataFrame_st.iterrows():
            local_ipv4 = None
            local_ipv6 = None
            # 2. 获取当前样本的scene,ID,storage_Add信息
            scene = row['scene']
            id = row['ID']
            storage_Add = row['storage_Add']
            # 3. 检查storage_Add目录下是否存在merged_csvFiles目录，如果不存在则打印警告信息并继续处理下一个样本记录
            merged_csv_dir = os.path.join(storage_Add, "merged_csvFiles")
            if not os.path.exists(merged_csv_dir):
                print(f"!!! {self.__name} - extractor_V1 Warning: The directory '{merged_csv_dir}' does not exist for record ID {id}. Skipping local IP extraction for this record.")
                continue
            print(f"### {self.__name} - extractor_v1 Info: Found directory '{merged_csv_dir}' for record ID {id}. Proceeding with local IP extraction for this record.")
            # 4. 在merged_csvFiles目录下搜索csv文件，目标文件的正则匹配格式为scene_ID_merged.csv等
            csv_pattern = re.compile(rf'merged_{scene}_{id}\.csv$', re.IGNORECASE)
            csv_files = []
            for root, dirs, files in os.walk(merged_csv_dir):
                # 遍历files列表，找到符合条件的csv文件
                for file in files:
                    if csv_pattern.match(file):
                        csv_files.append(os.path.join(root, file))
            # 如果在merged_csvFiles目录下没有找到csv文件，则打印警告信息并继续处理下一个样本记录
            if not csv_files:
                print(f"!!! {self.__name} - extractor_v1 Warning: No merged csv files found in the directory '{merged_csv_dir}' for record ID {id}. Skipping local IP extraction for this record.")
                continue
            print(f"### {self.__name} - extractor_v1 Info: Found {len(csv_files)} merged csv files in the directory '{merged_csv_dir}' for record ID {id}. Proceeding with local IP extraction for this record.")
            # 5. 将csv文件读入为DataFrame数据结构
            cache_df = pd.DataFrame()
            for csv_file in csv_files:
                try:
                    df = pd.read_csv(csv_file)
                    cache_df = pd.concat([cache_df, df], ignore_index=True)
                    print(f"### {self.__name} - extractor_v1 Info: Successfully read csv file '{csv_file}' into DataFrame for record ID {id}.")
                except Exception as e:
                    print(f"!!! {self.__name} - extractor_v1 Error: Failed to read csv file '{csv_file}' into DataFrame for record ID {id}. Error details: {e}. Skipping this csv file.")
                    continue
            # 6. 对cache_df进行时间排序
            if "frame.time_epoch" in cache_df.columns:
                cache_df.sort_values(by="frame.time_epoch", inplace=True)
                print(f"### {self.__name} - extractor_v1 Info: Successfully sorted the combined DataFrame by 'frame.time_epoch' for record ID {id}.")
            else:
                print(f"!!! {self.__name} - extractor_v1 Warning: The column 'frame.time_epoch' is missing from the combined DataFrame for record ID {id}. Unable to sort by time. Proceeding without sorting.")
                continue
            # 7. 首先搜索IPv4协议下的本机地址
            # 过滤所有协议为ipv4的记录
            ipv4_df = cache_df[cache_df['frame.protocols'].str.contains(':ip:', case=False, na=False)]
            # 对ipv4_df中的ip.src和ip.dst进行聚合统计，获取出现频率最高的前5个IP地址
            if 'ip.src' in ipv4_df.columns and 'ip.dst' in ipv4_df.columns:
                ip_counts = pd.concat([ipv4_df['ip.src'], ipv4_df['ip.dst']]).value_counts()
                top_ip_addresses = ip_counts.head(3).index.tolist()
                print(f"### {self.__name} - extractor_v1 Info: The top 3 most frequent IP addresses in the IPv4 protocol for record ID {id} are: {top_ip_addresses}.")
            else:
                print(f"!!! {self.__name} - extractor_v1 Warning: The columns 'ip.src' and/or 'ip.dst' are missing from the IPv4 DataFrame for record ID {id}. Unable to perform IP address frequency analysis. Skipping this record.")
                continue
            # 判断出现频率最高的IP地址是否为私有地址，如果是则认为该IP地址为本机IP地址
            private_ip_pattern = re.compile(r'^(10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|192\.168\.)')
            local_ip_addresses = [ip for ip in top_ip_addresses if private_ip_pattern.match(ip)]
            if local_ip_addresses:
                result = 0
                local_ipv4 = local_ip_addresses[0]
                print(f"### {self.__name} - extractor_v1 Info: The identified local IP addresses for record ID {id} are: {local_ipv4}.")
            else:
                print(f"!!! {self.__name} - extractor_v1 Warning: No local IP addresses were identified for record ID {id} based on the frequency analysis and private IP address pattern. The top IP addresses found were: {top_ip_addresses}.")
                result = -1
            
            # 8. 继续搜索IPv6协议下的本机地址
            # 过滤所有协议为ipv6的记录
            ipv6_df = cache_df[cache_df['frame.protocols'].str.contains(':ipv6:', case=False, na=False)]
            # 对ipv6_df中的ipv6.src和ipv6.dst进行聚合统计，获取出现频率最高的前5个IP地址
            if 'ipv6.src' in ipv6_df.columns and 'ipv6.dst' in ipv6_df.columns:
                ipv6_counts = pd.concat([ipv6_df['ipv6.src'], ipv6_df['ipv6.dst']]).value_counts()
                top_ipv6_addresses = ipv6_counts.head(3).index.tolist()
                print(f"### {self.__name} - extractor_v1 Info: The top 3 most frequent IP addresses in the IPv6 protocol for record ID {id} are: {top_ipv6_addresses}.")
            else:
                print(f"!!! {self.__name} - extracotr_v1 Warning: The columns 'ipv6.src' and/or 'ipv6.dst' are missing from the IPv6 DataFrame for record ID {id}. Unable to perform IP address frequency analysis. Skipping this record.")
                continue
            # 直接判定出现频率最高的IP地址是否为IPv6的本地地址
            local_ipv6 = top_ipv6_addresses[0]
            result = -1
            print(f"### {self.__name} - extractor_v1 Info: The identified local IPv6 address for record ID {id} is: {local_ipv6}.")

            # 9. 将搜索结果持久化存储在storage_Add目录下的localIP_info.txt文件中
            local_ip_info_path = os.path.join(storage_Add, "localIP_info.txt")
            try:
                with open(local_ip_info_path, 'w') as f:
                    f.write(f"Local IP extraction results for record ID {id}:\n")
                    f.write(f"Identified local IPv4 address: {local_ipv4}\n")
                    f.write(f"Identified local IPv6 address: {local_ipv6}\n")
                print(f"### {self.__name} Info: Successfully saved the identified local IP addresses to '{local_ip_info_path}' for record ID {id}.")
            except Exception as e:
                print(f"!!! {self.__name} Error: Failed to save the identified local IP addresses to '{local_ip_info_path}' for record ID {id}. Error details: {e}.")
                continue
        return result
    
    """
    @brief  从IPv4协议的IP-Port对中解析IP信息和Port信息
    @param  ip_port，IP-Port对的字串信息
    @return ip, port
    """
    def split_ipv4_port(self, ip_port):
        ip = ""
        port = ""

        ip, port = ip_port.split(":")

        return ip, port
    
    """
    @brief  从IPv6协议的IP-Port对中解析IP信息和Port信息
    @param  ip_port，IP-Port对的字串信息
    @return ip, port
    """
    def split_ipv6_port(self, ip_port):
        ip = ""
        port = ""

        ip, port = ip_port.rsplit(":", 1)

        return ip, port

    """
    @brief  从总览文件中抓取本机的IP地址
    @param  file_path 纵览文件的路径地址
    @return ip_list 记录着纵览文件中IP信息的列表
    """
    def capture_IP_from_file(self, file_path):
        ip_list = []
        # 逐行读取file中的信息
        try:
            with open(file_path) as f:
                counter = 0
                for line in f:
                    # 由于overview文件的末端将会引入大量的干扰，我们仅仅考虑前部分行内的内容
                    if counter >= 30:
                        break
                    # 锁定携带本机IP地址的行内容
                    if "<->" in line:
                        parts = line.split()
                        for part in parts:
                            ip = ""
                            port = ""
                            # 由于overview中IP地址还携带了端口信息，我们需要滤除端口信息，保留IP信息
                            if part.count(':') == 1:
                                # 该信息极有可能是IPv4地址的IP-Port对
                                ip, port = self.split_ipv4_port(part)
                            elif part.count(':') > 1:
                                # 该信息极有可能是IPv6地址的IP-Port对
                                ip, port = self.split_ipv6_port(part)
                            # 判定保留的信息是否是IP地址，并且在overview文件中，本机地址往往是第一个出现的地址，第二个地址是远端服务地址
                            if (is_ipv4(ip) 
                                and (ip.startswith("10.") 
                                     or ip.startswith("172.")
                                     or ip.startswith("192.168."))
                            ):
                                # 我们将会载入非重复的IP信息
                                if not ip in ip_list:
                                    ip_list.append(ip)
                                break # 默认每一行出现的第一个复合IP协议的内容就是本机地址
                            elif is_ipv6(ip):
                                # 我们将会载入非重复的IP信息
                                if not ip in ip_list:
                                    ip_list.append(ip)
                                break # 默认每一行出现的第一个复合IP协议的内容就是本机地址
                    counter +=1
        except Exception as e:
            print(f"!!! {self.__name} capture_IP Error: Read Action with Error: {e}")

        return ip_list

    def extract(self, *args, **kwargs) -> any:
        result = 0
        # 本机地址的提取工作开始
        # 遍历legal_dataFrame_st中的各个样本
        for index, row in self.__leagal_dataFrame_st.iterrows():
            # 获取必要信息
            id = row["ID"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]
            # 检查 storage_Add 目录是否存在
            if not os.path.exists(storage_Add):
                print(f"!!! {self.__name} - extract Error: cannot find legal storage path in sample: {scene}-{id}.")
                result -= 1
                continue
            
            # 定位overview目录，将搜索到的目标文件载入列表
            overview_dir = os.path.join(storage_Add, "overview_extractor")
            if not os.path.exists(overview_dir):
                print(f"!!! {self.__name} - extract Error: There is no overview_extractor Dir in sample:{scene}-{id}'s storage path.")
                print(f"!!! {self.__name} - extract Error: current version need overview_extractor before localIP extractor.")
                print(f"!!! {self.__name} - extract Error: so if you want to use command:-l, please add '-o'.")
                result -= 1
                continue
            udp_overview_path = os.path.join(overview_dir, "udp_overview.txt")
            tcp_overview_path = os.path.join(overview_dir, "tcp_overview.txt")
            targetFile_lists = []
            if os.path.exists(udp_overview_path):
                targetFile_lists.append(udp_overview_path)
            if os.path.exists(tcp_overview_path):
                targetFile_lists.append(tcp_overview_path)
            # 对搜索结果进行输出
            if len(targetFile_lists) == 0:
                print(f"!!! {self.__name} - extract Warning: There is no executable file in sample:{scene}-{id}'s overview_extractor.")
                result -= 1
                continue
            print(f"### {self.__name} - extract Info: Found these executable file in sample:{scene}-{id}'s overview_extractor:")
            for file in targetFile_lists:
                print(f"--->{os.path.basename(file)}")
            
            # 开始解析各个overview当中反馈的本机地址信息
            IP_map = {} # 暂存总览文件中本机IP的字典结构
            localIP_list = [] # 暂存搜索得到的本机IP
            for file in targetFile_lists:
                IP_list = []
                IP_list = self.capture_IP_from_file(file)
                IP_map[f"{os.path.basename(file)}"] = IP_list
            if len(IP_map) == 0:
                print(f"!!! {self.__name} extract Warning: There is effective IP in file: {os.path.basename(file)}")
                result -= 1
                continue
            # 对各个总览文件中的本机IP进行聚合统计
            for filename, iplist in IP_map.items():
                for ip in iplist:
                    if not ip in localIP_list:
                        localIP_list.append(ip)
            if len(localIP_list) == 0:
                print(f"!!! {self.__name} extract Warning: sample {scene}-{id} have no effective local ip.")
                resutl -= 1
                continue

            # 对收集到的本机IP进行：分离IPv4地址和IPv6地址，并输出到指定路径中
            localIPv4_list = []
            localIPv6_list = []
            for ip in localIP_list:
                if is_ipv4(ip):
                    localIPv4_list.append(ip)
                elif is_ipv6(ip):
                    localIPv6_list.append(ip)
                else:
                    print(f"!!! {self.__name} - extractor Warning: {ip} is neither ipv4 or ipv6")
            # 锁定并创建目标输出目录的地址
            localIP_output_dir = os.path.join(storage_Add, "localIP_extractor")
            try:
                os.makedirs(localIP_output_dir)
                print(f"### {self.__name} - extractor Info: Have Generated output directory in {localIP_output_dir}.")
            except Exception as e:
                print(f"!!! {self.__name} - extractor: In localIP_output_dir generating, there is some errors: {e}.")
            # 构建输出文件地址，并展开输出
            ipv4_file = os.path.join(localIP_output_dir, "ipv4.txt")
            ipv6_file = os.path.join(localIP_output_dir, "ipv6.txt")
            try:
                with open(ipv4_file, 'w') as f:
                    for ip in localIPv4_list:
                        f.write(f"{ip}\n")
            except Exception as e:
                print(f"!!! {self.__name} - extract Error: When writing to ipv4.txt, Errors: {e}.")
            try:
                with open(ipv6_file, 'w') as f:
                    for ip in localIPv6_list:
                        f.write(f"{ip}\n")
            except Exception as e:
                print(f"!!! {self.__name} - extract Error: When writing to ipv6.txt, Errors: {e}.")

        if(result == 0):
            print(f"### {self.__name} - extractor Info: Finished local IP extraction.")

        return 0

    def toString(self) -> str:
        return f"{self.__name}"