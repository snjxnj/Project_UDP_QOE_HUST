import os
import sys
import re
import matplotlib as mp
import pandas as pd
from visual_06.udp_visual import udp_visualizer

"""
2026.04.07
需要特别说明的是：
    1. visual脚本可以单独使用，即针对某个样本结果进行单独地可视化绘制
    2. visual脚本的代码可以被main脚本复用，以便在数据生成过程中便可进行可视化生成与可视化持久
"""


"""
@brief  UDP数据的可视化操作
@param  样本地址
"""


def is_directory_empty(directory_path):
    """
    检查目录是否为空（不包含隐藏文件/目录）
    返回: 1 为空, 0 不为空, 其他值非法
    """
    if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
        return -1  # 路径不存在或不是目录，返回0
    
    # 获取目录内容
    entries = os.listdir(directory_path)
    
    # 过滤掉隐藏文件/目录（以 . 开头）
    non_hidden_entries = [entry for entry in entries if not entry.startswith('.')]
    
    # 判断是否为空
    return 1 if len(non_hidden_entries) == 0 else 0

if __name__ == "__main__":
    print(f"### Visual.py Info: Visualizing...")

    # 1. 获取用户操作信息：用户将会使用python visual.py -u -m ... <样本地址>
    # 获取样本地址（最后一个参数）
    sample_path = sys.argv[-1] if len(sys.argv) > 1 else None
    # 获取用户指令（除了文件名和最后一个参数之外的所有参数）
    commands = sys.argv[1:-1] if len(sys.argv) > 2 else []
    # 反馈接收的用户操作信息
    print(f"### Visual.py Info: sample path: {sample_path}")
    print(f"### Visual.py Info: user commands: {commands}")
    print("-"*30 + "\n")

    # 2. 检查用户给出样本地址的合法性，并搜寻能够可视化的数据内容
    datas = {}
    if os.path.exists(sample_path):
        print(f"### Visual.py Info: sample path exists...")
        # 2.1 检查UDP数据库是否存在
        udp_dir = os.path.join(sample_path, "udp_extractor")
        if not is_directory_empty(udp_dir):
            print(f"### Visual.py Info: Have Found UDP-Datas for visualization.")
            datas["udp"] = udp_dir
        # 2.2 检查Modem数据库是否存在，待更新
    
    else:
        print(f"!!! Visual.py Error: sample path fatal Error: ilegal path!")
    # 2.3 检查datas是否具备有效信息
    if len(datas) == 0:
        print(f"!!! Visual.py Error: Cannot Find any effective datas in sample-path: {sample_path}!")
        sys.exit(0)
    
    # 3. 遍历用户地址
    for command in commands:
        if command == "-u":
            # 3.1 执行样本信息下UDP数据的可视化操作
            udp_visualizer = udp_visualizer(sample_path)
            udp_visualizer.visual_in_windows()
        else:
            print(f"!!! Visual.py Warning: Unknown command of: {command}.")