import os
import csv
import subprocess
from pathlib import Path
import src.add_list.Listparser as Parser
import re

import src.pcap.tshark.operation as op

"""
这个脚本负责统计归并的.pcap文件中ipv4/ipv6地址在通信过程中的占比
ipv4：
tshark -q -n -r <.pcap> -z  ip_hosts,tree
ipv6：
tshark -q -n -r <.pcap> -z ipv6_hosts,tree

主要需要调动tshark工具：其会在控制台打印如下信息
==============================================================================================================================================
IPv4 Statistics / All Addresses:
Topic / Item                   Count         Average       Min Val       Max Val       Rate (ms)     Percent       Burst Rate    Burst Start
----------------------------------------------------------------------------------------------------------------------------------------------
IPv4 Statistics/All Addresses  62080                                                   0.0504        100%          1.6900        251.904
 10.159.62.41                  61985                                                   0.0503        99.85%        1.6900        251.904
 36.155.220.17                 38223                                                   0.0310        61.57%        1.1200        251.917
 36.155.223.152                4596                                                    0.0037        7.40%         0.9300        943.836
 111.48.8.188                  2722                                                    0.0022        4.38%         0.2800        24.046
 183.255.204.79                1449                                                    0.0012        2.33%         0.1600        81.524
 36.131.217.237                1275                                                    0.0010        2.05%         0.9900        1021.385
 ......
"""

def save_ipv4_ranks_to_csv(
    dir_path: str,
    output_path: str,
    pcap_name: str = "temp_udp.pcap",
    csv_name: str = "ipv4_hosts_rank.csv",
    max_rows: int = 10,   # 只保存前 N 个 IP
) -> Path:
    """
    使用 tshark 统计 IPv4 主机占比排名：
        tshark -q -n -r temp_udp.pcap -z ip_hosts,tree

    按输出顺序取前 max_rows 个 IP，保存到 CSV。
    """
    p = Path(dir_path)

    if not p.exists():
        print(f"目录不存在: {dir_path}")
        return
    if not p.is_dir():
        print(f"不是目录: {dir_path}")
        return

    pcap_file = p / pcap_name
    if not pcap_file.exists():
        print(f"pcap 文件不存在: {pcap_file}")
        return

    cmd = [
        "tshark",
        "-q",
        "-n",
        "-r",
        str(pcap_file),
        "-z",
        "ip_hosts,tree",
    ]

    result = subprocess.run(
        cmd,
        cwd=str(p),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("运行 tshark 失败:", result.stderr)
        return

    lines = result.stdout.splitlines()
    hosts = []

    ipv4_re = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        # 跳过分隔线、标题和总计行
        if line.startswith("=") or line.startswith("-"):
            continue
        if line.startswith("IPv4 Statistics") or line.startswith("Topic / Item"):
            continue

        parts = line.split()
        if len(parts) < 2:
            continue

        # 第一列是 IPv4 才认为是我们要的行
        if not ipv4_re.match(parts[0]):
            continue

        ip = parts[0]
        count_str = parts[1]

        try:
            count = int(count_str)
        except ValueError:
            continue

        # 后面列可能有缺失，这里按“最后四列”为 Rate / Percent / Burst Rate / Burst Start
        rate = percent = burst_rate = burst_start = ""
        if len(parts) >= 6:
            rate = parts[-4]         # Rate (ms)
            percent = parts[-3]      # Percent，形如 "99.85%"
            burst_rate = parts[-2]   # Burst Rate
            burst_start = parts[-1]  # Burst Start

        hosts.append([
            ip,
            count,
            rate,
            percent,
            burst_rate,
            burst_start,
        ])

        if max_rows is not None and len(hosts) >= max_rows:
            break

    if not hosts:
        print("没有解析到 IPv4 主机统计信息")
        return

    csv_path = Path(output_path) / csv_name
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "rank",
            "ip",
            "count",
            "rate",
            "percent",
            "burst_rate",
            "burst_start",
        ])
        for i, h in enumerate(hosts, start=1):
            writer.writerow([i] + h)

    print(f"IPv4 主机排名已保存到: {csv_path}")
    """# 删除用于统计的 merged.pcap，避免占用过多空间
    try:
        pcap_file.unlink()  # 等价于 os.remove(pcap_file)
        print(f"已删除临时文件: {pcap_file}")
    except FileNotFoundError:
        print(f"未找到要删除的文件: {pcap_file}")
    except PermissionError:
        print(f"没有权限删除文件: {pcap_file}") """
    
    return csv_path

def save_ipv6_ranks_to_csv(
    dir_path: str,
    output_path: str,
    pcap_name: str = "temp_udp.pcap",
    csv_name: str = "ipv6_hosts_rank.csv",
    max_rows: int = 10,   # 只保存前 N 个 IP
) -> Path:
    """
    使用 tshark 统计 IPv6 主机占比排名：
        tshark -q -n -r temp_udp.pcap -z ipv6_hosts,tree

    按输出顺序取前 max_rows 个 IP，保存到 CSV。
    """
    p = Path(dir_path)

    if not p.exists():
        print(f"目录不存在: {dir_path}")
        return
    if not p.is_dir():
        print(f"不是目录: {dir_path}")
        return

    pcap_file = p / pcap_name
    if not pcap_file.exists():
        print(f"pcap 文件不存在: {pcap_file}")
        return

    cmd = [
        "tshark",
        "-q",
        "-n",
        "-r",
        str(pcap_file),
        "-z",
        "ipv6_hosts,tree",
    ]

    result = subprocess.run(
        cmd,
        cwd=str(p),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("运行 tshark 失败:", result.stderr)
        return

    lines = result.stdout.splitlines()
    hosts = []

    # 简单 IPv6 正则（包含 : 和十六进制字符）
    ipv6_re = re.compile(r"^[0-9a-fA-F:]+$")

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        # 跳过分隔线、标题和总计行
        if line.startswith("=") or line.startswith("-"):
            continue
        if line.startswith("IPv6 Statistics") or line.startswith("Topic / Item"):
            continue

        parts = line.split()
        if len(parts) < 2:
            continue

        # 第一列是 IPv6 地址才认为是我们要的行
        if not ipv6_re.match(parts[0]):
            continue

        ip = parts[0]
        count_str = parts[1]

        try:
            count = int(count_str)
        except ValueError:
            continue

        # 后面列可能有缺失，这里按“最后四列”为 Rate / Percent / Burst Rate / Burst Start
        rate = percent = burst_rate = burst_start = ""
        if len(parts) >= 6:
            rate = parts[-4]         # Rate (ms)
            percent = parts[-3]      # Percent
            burst_rate = parts[-2]   # Burst Rate
            burst_start = parts[-1]  # Burst Start

        hosts.append([
            ip,
            count,
            rate,
            percent,
            burst_rate,
            burst_start,
        ])

        if max_rows is not None and len(hosts) >= max_rows:
            break

    if not hosts:
        print("没有解析到 IPv6 主机统计信息")
        return

    csv_path = Path(output_path) / csv_name
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "rank",
            "ip",
            "count",
            "rate",
            "percent",
            "burst_rate",
            "burst_start",
        ])
        for i, h in enumerate(hosts, start=1):
            writer.writerow([i] + h)

    print(f"IPv6 主机排名已保存到: {csv_path}")
    """# 删除用于统计的 merged.pcap，避免占用过多空间
    try:
        pcap_file.unlink()  # 等价于 os.remove(pcap_file)
        print(f"已删除临时文件: {pcap_file}")
    except FileNotFoundError:
        print(f"未找到要删除的文件: {pcap_file}")
    except PermissionError:
        print(f"没有权限删除文件: {pcap_file}")
    """
    return csv_path

def check_is_private_ipv4(ip: str) -> bool:
    """
    判断 IPv4 是否在“私有地址段”里：
      - 10.*.*.*
      - 192.168.*.*
      - 172.16.0.0 ~ 172.31.255.255
    """
    try:
        parts = list(map(int, ip.split(".")))
    except ValueError:
        return False  # 不是合法 IPv4

    if len(parts) != 4:
        return False

    a, b, c, d = parts

    if a == 10:
        return True

    if a == 192 and b == 168:
        return True

    if a == 172 and 16 <= b <= 31:
        return True

    return False

def summarize_host_and_targets(
    ipv4_csv_path: str | Path | None,
    ipv6_csv_path: str | Path | None,
    rate_threshold: float = 9.0,
    percent_threshold: float = 10.0,
) -> list[str]:
    """
    返回: [host_v4, dst_v4_list, host_v6, dst_v6_list]
    在本函数内部：
      - 先按 rate/percent 初筛 dst_v4_list/dst_v6_list
      - 再根据 host_v4_count / host_v6_count 决定使用 v4、v6 还是都用
    """

    def _extract_from_csv(path: str | Path | None) -> tuple[str, list[str], int]:
        host_ip = ""
        host_count = 0
        dst_ips: list[str] = []

        if not path:
            return host_ip, dst_ips, host_count

        p = Path(path)
        if not p.exists() or not p.is_file():
            return host_ip, dst_ips, host_count

        with p.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if not header:
                return host_ip, dst_ips, host_count

            first_data_row = next(reader, None)
            if not first_data_row:
                return host_ip, dst_ips, host_count

            # rank, ip, count, rate, percent, burst_rate, burst_start
            host_ip = first_data_row[1].strip()
            try:
                host_count = int(first_data_row[2])
            except (IndexError, ValueError):
                host_count = 0

            for row in reader:
                if len(row) < 5:
                    continue
                ip = row[1].strip()
                rate_str = row[3].strip()
                percent_str = row[4].strip()

                include = False

                if rate_str:
                    try:
                        rate_ms = float(rate_str)
                        if rate_ms * 1000 >= rate_threshold:
                            include = True
                    except ValueError:
                        pass

                if (not include) and percent_str:
                    if percent_str.endswith("%"):
                        percent_str = percent_str[:-1]
                    try:
                        percent_val = float(percent_str)
                        if percent_val >= percent_threshold:
                            include = True
                    except ValueError:
                        pass

                if include:
                    dst_ips.append(ip)

        return host_ip, dst_ips, host_count

    host_v4, dst_v4_list, host_v4_count = _extract_from_csv(ipv4_csv_path)
    host_v6, dst_v6_list, host_v6_count = _extract_from_csv(ipv6_csv_path)

    # 根据主机 count 比例决定使用模式
    use_v4 = bool(host_v4) and bool(dst_v4_list)
    use_v6 = bool(host_v6) and bool(dst_v6_list)

    if host_v4_count > 0 and host_v6_count > 0:
        ratio_v4_v6 = host_v4_count / host_v6_count
        ratio_v6_v4 = host_v6_count / host_v4_count

        # 有一个协议的 count 只有另一个的 <10%，只用“占大头”的这个
        if ratio_v4_v6 < 0.1:
            use_v4 = False
        elif ratio_v6_v4 < 0.1:
            use_v6 = False

    # 把不使用的协议直接置空，后面 filter_ip_rules 只在非空的上面工作
    if not use_v4:
        host_v4 = ""
        dst_v4_list = []
    if not use_v6:
        host_v6 = ""
        dst_v6_list = []

    return [host_v4, dst_v4_list, host_v6, dst_v6_list]

def filter_ip_rules(scene: str, ip_list):
    # [host_v4, dst_v4_list, host_v6, dst_v6_list]
    host_v4_str, dst_v4_list, host_v6_str, dst_v6_list = ip_list

    dst_v4_list = dst_v4_list or []
    dst_v6_list = dst_v6_list or []

    host_v4_list: list[str] = []
    if host_v4_str:
        host_v4_list.append(host_v4_str)

    new_dst_v4_list: list[str] = []

    for ip in dst_v4_list:
        if check_is_private_ipv4(ip):
            if ip not in host_v4_list:
                host_v4_list.append(ip)
        else:
            if ip not in new_dst_v4_list:
                new_dst_v4_list.append(ip)

    host_v4_joined = "+".join(host_v4_list) if host_v4_list else ""
    host_v6_joined = host_v6_str or ""

    dst_v4_joined = "+".join(new_dst_v4_list) if new_dst_v4_list else ""
    dst_v6_joined = "+".join(dst_v6_list) if dst_v6_list else ""

    # 如果没有对应的目标机，则不保留这类主机
    if not dst_v4_joined:
        host_v4_joined = ""
    if not dst_v6_joined:
        host_v6_joined = ""

    local_parts = []
    if host_v4_joined:
        local_parts.append(host_v4_joined)
    if host_v6_joined:
        local_parts.append(host_v6_joined)
    local_ip_str = "+".join(local_parts) if local_parts else ""

    server_parts = []
    if dst_v4_joined:
        server_parts.append(dst_v4_joined)
    if dst_v6_joined:
        server_parts.append(dst_v6_joined)
    server_ip_str = "+".join(server_parts) if server_parts else ""

    return [local_ip_str, server_ip_str]

def get_host_server_ip(
        src_add:str, 
        ID:str, 
        scene:str,
        ipv4_dir:str,
        ipv6_dir:str
        ):
    r"""
    利用tshark分析主机和目的机ip，需填写：每个样本的/export_*路径，addlist中的ID值，
    业务名称，tshark分析结果存放的csv文件目录路径，最终返回[local_ip,serv_ip]
    """
    print(f'\n正统计分析{scene}_{ID}的IP地址')
    dirpath = os.path.join(src_add, 'bbklog', 'netlog')
    csvname = scene + '_' + ID[:10] + '.csv'
    op.delete_merged_pcap_files(root_dir=dirpath)    # 可以预先清一遍缓存文件 避免无法merge
    op.merge_pcap_files(dir_path=dirpath)
    op.filter_udp_non_dns_pcap(dir_path=dirpath)
    # 导出统计结果
    ipv4_rk_csv = save_ipv4_ranks_to_csv(dir_path=dirpath,
                              output_path=ipv4_dir,
                              csv_name=csvname)  
    ipv6_rk_csv = save_ipv6_ranks_to_csv(dir_path=dirpath,
                              output_path=ipv6_dir,
                              csv_name=csvname)  
    op.delete_merged_pcap_files(root_dir=dirpath)    # 清缓存 节省空间
    result_list = summarize_host_and_targets(ipv4_rk_csv,ipv6_rk_csv)
    print(f'ip分析-初步提取分析结果：{result_list}')
    final_res = filter_ip_rules(scene, result_list)

    print(f'ip分析-最终尝试使用：{final_res}')
    return final_res

if __name__ == "__main__":
    # src_Add+bbklog+netlog
    addlist_path = r'D:\XFC_files\code\UDP2026\config\address_List-set2.txt'
    addlist = Parser.SampleListParser(addlist_path)
    r"""
    process_cnt = 0
    for sample in addlist.samples:
        # 遍历addlist中的每一行
        process_cnt += 1
        print(f'\n正处理第{process_cnt}行：')
        dirpath = os.path.join(sample['src_Add'], 'bbklog', 'netlog')
        csvname = addlist.use_sample_id(sample) + '.csv'
        op.delete_merged_pcap_files(root_dir=dirpath)    # 可以预先清一遍缓存文件 避免无法merge
        op.merge_pcap_files(dir_path=dirpath)
        save_ipv4_ranks_to_csv(dir_path=dirpath,
                              output_path=r"D:\XFC_files\code\UDP2026\src\pcap\change\v4",
                              csv_name=csvname)  
                              """
    print()

"""
针对少数样本的异常状况做出调整：
meeting2025103101 变ipv4： 10.46.26.89+10.44.208.73

2026.3.16:
思考了一下 ip排行可以先进行udp包过滤再查看
"""