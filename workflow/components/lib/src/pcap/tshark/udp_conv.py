import os
import csv
import subprocess
from pathlib import Path
import src.add_list.Listparser as Parser
import re

import src.pcap.tshark.operation as op

def analyze_udp_conversations(dir_path: str, pcap_name: str = "merged.pcap", max_lines: int = 14) -> None:
    """
    在给定目录下运行：
        tshark -n -q -r merged.pcap -z conv,udp | Select-Object -First N
    只保留表头 + 前几行记录。最终打印在控制台，无导出，仅做检测
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
        "powershell",
        "-NoLogo",
        "-NoProfile",
        "-Command",
        f'tshark -n -q -r "{pcap_name}" -z conv,udp | Select-Object -First {max_lines}'
    ]

    try:
        subprocess.run(cmd, cwd=str(p), check=True)
    except subprocess.CalledProcessError as e:
        print("运行 tshark 失败:", e)

def save_udp_conversations_to_csv(
    dir_path: str,
    output_path:str,
    pcap_name: str = "merged.pcap",
    csv_name: str = "udp_conversations.csv",
    max_rows: int = 10,   # 只保存前 N 条会话
) -> None:
    """
    把经过控制台输入 tshark -n -q -r merged.pcap -z conv,udp | Select-Object -First N命令后的内容整合成.csv
    dir_path：.pcap文件所在的文件目录
    output_path:输出.csv所在目录
    """
    p = Path(dir_path)

    if not p.exists():
        print(f"目录不存在: {dir_path}")
        return
    if not p.is_dir():
        print(f"不是目录: {dir_path}")
        return

    pcap_file = p / pcap_name   # merge后的文件
    if not pcap_file.exists():
        print(f"整合后的merged.pcap文件不存在，需先进行整合: {pcap_file}")
        return

    # 直接跑 tshark，拿到完整 conv,udp 输出
    cmd = [
        "tshark",
        "-n",
        "-q",
        "-r",
        str(pcap_file),
        "-z",
        "conv,udp",
    ]
    result = subprocess.run(
        cmd,
        cwd=str(p),          # 在该目录下运行
        capture_output=True, # 抓 stdout/stderr
        text=True,
    )

    if result.returncode != 0:
        print("运行 tshark 失败:", result.stderr)
        return

    # result.stdout是 tshark 在标准输出里打印的整段文本
    lines = result.stdout.splitlines()
    conversations = []

    for line in lines:
        line = line.rstrip()
        if not line.strip():
            continue
        # 跳过分隔线和标题行
        if line.startswith("=") or line.startswith("UDP ") or line.startswith("Filter:"):
            continue
        if line.strip().startswith("|"):
            # 就是你发的那两行表头
            continue
        # 真正的会话行一定包含 "<->"
        if "<->" not in line:
            continue

        # 拆成 左地址 / 右地址+后面数值
        left, rest = line.split("<->", 1)
        addr_a = left.strip()
        rest = rest.strip()

        parts = rest.split()
        # 预期格式（示例）：
        # addr_b   48388 26 MB   122769 133 MB   171157 160 MB   1077.413700000 886.2113
        if len(parts) < 12:
            # 保险：格式不对就跳过
            continue

        addr_b = parts[0]

        frames_a = parts[1]
        bytes_a = " ".join(parts[2:4])        # "26 MB" / "0 bytes"

        frames_b = parts[4]
        bytes_b = " ".join(parts[5:7])

        frames_total = parts[7]
        bytes_total = " ".join(parts[8:10])

        rel_start = parts[10]
        duration = parts[11]

        conversations.append([
            addr_a,
            addr_b,
            frames_a,
            bytes_a,
            frames_b,
            bytes_b,
            frames_total,
            bytes_total,
            rel_start,
            duration,
        ])

        if max_rows is not None and len(conversations) >= max_rows:
            break

    if not conversations:
        print("没有解析到 UDP 会话行")
        return

    csv_path = Path(output_path) / csv_name
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "addr_a",
            "addr_b",
            "frames_a",
            "bytes_a",
            "frames_b",
            "bytes_b",
            "frames_total",
            "bytes_total",
            "rel_start",
            "duration",
        ])
        writer.writerows(conversations)

    print(f"已保存到: {csv_path}")
    # 删除用于统计的 merged.pcap，避免占用过多空间
    try:
        pcap_file.unlink()  # 等价于 os.remove(pcap_file)
        print(f"已删除临时文件: {pcap_file}")
    except FileNotFoundError:
        print(f"未找到要删除的文件: {pcap_file}")
    except PermissionError:
        print(f"没有权限删除文件: {pcap_file}")

if __name__ == "__main__":
    # src_Add+bbklog+netlog
    addlist_path = r'D:\XFC_files\code\UDP2026\config\address_List-set1.txt'
    addlist = Parser.SampleListParser(addlist_path)

    process_cnt = 0
    for sample in addlist.samples:
        # 遍历addlist中的每一行
        process_cnt += 1
        print(f'\n正处理第{process_cnt}行：')
        dir = os.path.join(sample['src_Add'], 'bbklog', 'netlog')
        csvname = addlist.use_sample_id(sample) + '.csv'
        op.delete_merged_pcap_files(root_dir=dir)    # 可以预先清一遍缓存文件 避免无法merge
        op.merge_pcap_files(dir_path=dir)
        save_udp_conversations_to_csv(
                dir_path = dir,
                output_path = r'D:\XFC_files\code\UDP2026\src\pcap\tshark\udp_conv',
                csv_name = csvname,
            )