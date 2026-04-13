import os
import csv
import subprocess
from pathlib import Path
import src.add_list.Listparser as Parser
import re

def list_pcap_files(dir_path: str) -> None:
    """
    在给定目录下调用 PowerShell:
        Get-ChildItem *.pcap*
    并把结果输出到控制台。
    """
    p = Path(dir_path)

    if not p.exists():
        print(f"目录不存在: {dir_path}")
        return
    if not p.is_dir():
        print(f"不是目录: {dir_path}")
        return

    # 在该目录下运行 PowerShell 命令
    cmd = [
        "powershell",
        "-NoLogo",
        "-NoProfile",
        "-Command",
        "Get-ChildItem *.pcap*"
    ]
    subprocess.run(cmd, cwd=str(p))

def delete_merged_pcap_files(root_dir: str) -> None:
    """
    删除给定目录（含子目录）下的中间 pcap 文件：
      1) merged.pcap
      2) temp_开头的文件（例如 temp_udp.pcap）
    """
    root = Path(root_dir)

    if not root.exists():
        print(f"目录不存在: {root_dir}")
        return
    if not root.is_dir():
        print(f"不是目录: {root_dir}")
        return

    deleted = 0
    seen = set()

    patterns = ["merged.pcap", "temp_*"]
    for pattern in patterns:
        for pcap in root.rglob(pattern):
            # 避免重复删除同一路径
            pcap_key = str(pcap.resolve())
            if pcap_key in seen:
                continue
            seen.add(pcap_key)

            if not pcap.is_file():
                continue

            try:
                pcap.unlink()
                deleted += 1
                print(f"已删除: {pcap}")
            except PermissionError:
                print(f"没有权限删除: {pcap}")
            except OSError as e:
                print(f"删除失败 {pcap}: {e}")

    print(f"共删除中间文件 {deleted} 个（merged.pcap + temp_*）")

def merge_pcap_files(dir_path: str, output_name: str = "merged.pcap") -> None:
    """
    在给定目录下运行：
        mergecap -w merged.pcap *.pcap*
    把目录中的所有 *.pcap* 合并成一个merged.pcap文件。
    """
    p = Path(dir_path)

    if not p.exists():
        print(f"目录不存在: {dir_path}")
        return
    if not p.is_dir():
        print(f"不是目录: {dir_path}")
        return

    cmd = [
        "powershell",
        "-NoLogo",
        "-NoProfile",
        "-Command",
        f'mergecap -w "{output_name}" *.pcap*'
    ]

    try:
        subprocess.run(cmd, cwd=str(p), check=True)
        print(f"合并完成，生成文件: {p / output_name}")
    except subprocess.CalledProcessError as e:
        print("运行 mergecap 失败:", e)

def filter_udp_non_dns_pcap(
    dir_path: str,
    input_name: str = "merged.pcap",
    output_name: str = "temp_udp.pcap",
) -> Path | None:
    """
    对指定.pcap过滤udp包（不包含dns包）
    在指定目录执行：
        tshark -r merged.pcap -Y "udp and not udp.port == 53" -w temp_udp.pcap
    返回输出文件路径；失败返回 None。
    """
    p = Path(dir_path)

    if not p.exists():
        print(f"目录不存在: {dir_path}")
        return None
    if not p.is_dir():
        print(f"不是目录: {dir_path}")
        return None

    input_file = p / input_name
    if not input_file.exists():
        print(f"输入文件不存在: {input_file}")
        return None

    output_file = p / output_name

    cmd = [
        "tshark",
        "-r",
        str(input_file),
        "-Y",
        "udp and not udp.port == 53",
        "-w",
        str(output_file),
    ]

    try:
        subprocess.run(cmd, cwd=str(p), check=True, capture_output=True, text=True)
        print(f"过滤完成，生成文件: {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        err = e.stderr.strip() if e.stderr else str(e)
        print(f"运行 tshark 过滤失败: {err}")
        return None