import os
import re
import sys
from ipaddress import IPv4Address, IPv6Address, ip_address

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


if __name__ == "__main__":
    load_str = "10.19.11.109:49942         <-> 42.187.184.206:8001         163041 119 MB      53401 8683 kB    216442 128 MB      749.580775000      1171.3886"
    down_load, up_load, total_load = get_load_from_line(load_str)
    print(f"down_load:  {down_load}B")
    print(f"up_load:    {up_load}B")
    print(f"total_load: {total_load}B")
    print("-------------------------------------------------------")

    local_ip, serv_ip = get_IPpair_from_line(load_str)
    print(f"local_ip:    {local_ip}")
    print(f"serv_ip:     {serv_ip}")
    print("-------------------------------------------------------")
