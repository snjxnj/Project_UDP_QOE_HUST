import os
import re
import sys
import pandas as pd
import datetime as dt
from .extractor import Extractor

class overview_extractor(Extractor):
    __name = "overview_extractor"
    __legal_dataFrame_st = pd.DataFrame()

    def __init__(self, legal_dataFrame_st):
        print(f"### {self.__name} Info: initializing.")
        self.__legal_dataFrame_st = pd.DataFrame()
        # 判定输入的DF数据结构是否为空
        if legal_dataFrame_st.empty:
            print(f"!!! {self.__name} Error: the input legal_dataFrame_st is empty!")
            return
        # 判定输入的DF数据结构是否包含必须信息
        if not ('ID' in legal_dataFrame_st.columns
            and 'scene' in legal_dataFrame_st.columns
            and 'storage_Add' in legal_dataFrame_st.columns):
            # print(f"!!! {self.__name} Error: the intput legal_dataFrame_st didnot have columns in ID, scene, storage_Add")
            if not ('ID' in legal_dataFrame_st.columns):
                print(f"!!! {self.__name} Error: the intput legal_dataFrame_st didnot have columns of ID")
            if not ('scene' in legal_dataFrame_st.columns):
                print(f"!!! {self.__name} Error: the intput legal_dataFrame_st didnot have columns of scene")
            if not ('storage_Add' in legal_dataFrame_st.columns):
                print(f"!!! {self.__name} Error: the intput legal_dataFrame_st didnot have columns of storage_Add")
            return
        self.__legal_dataFrame_st = legal_dataFrame_st

    """
    @brief  提取UDP分类下的总览信息
    @param  src_path 源数据pcap文件的路径信息
    @param  output_path 总览信息的导出文件路径
    @return int值，表示是否成功获取
    """
    def udp_overview(self, src_path, output_path):
        # tshark下进行聚合统计的指令内容
        command = (
            f'tshark -n -q -r {src_path} -z conv,udp > "{output_path}"'
        )
        # 执行tshark指令
        try:
            os.system(command)
            print(f"### {self.__name} - udp_overview Info: Finished udp_overview's tshark command")
        except Exception as e:
            print(f"!!! {self.__name} - udp_overview Error: {e}")
            return -1
        return 0

    """
    @brief  提取TCP分类下的总览信息
    @param  src_path 源数据pcap文件的路径信息
    @param  output_path 总览信息的导出文件路径
    @return int值，表示是否成功获取
    """
    def tcp_overview(self, src_path, output_path):
        # tshark下进行聚合统计的指令内容
        command = (
            f'tshark -n -q -r {src_path} -z conv,tcp > "{output_path}"'
        )
        # 执行tshark指令
        try:
            os.system(command)
            print(f"### {self.__name} - tcp_overview Info: Finished tcp_overview's tshark command")
        except Exception as e:
            print(f"!!! {self.__name} - tcp_overview Error: {e}")
            return -1
        return 0


    def extract(self, *args, **kwargs):
        result = 0
        # 开始提取各个样本的总览数据信息
        # 遍历各个样本
        for index, row in self.__legal_dataFrame_st.iterrows():
            # 获取当前样本的必要信息
            id = row["ID"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]
            # 确定仓库目录的合法性
            if not os.path.exists(storage_Add):
                print(f"!!! {self.__name} - extract Error: There is no storage_Add:{storage_Add}")
                result -= 1
                continue

            # 寻找构建overview的源文件-merged_capfile是否存在
            merged_capfile_dir = os.path.join(storage_Add, "merged_capFiles")
            if not os.path.exists(merged_capfile_dir):
                print(f"!!! {self.__name} - extract Error: cannot find merged capfile Dir in sample: {scene}-{id}")
                result -= 1
                continue
            merged_capfile_path = os.path.join(merged_capfile_dir, f"merged_{scene}_{id}.pcap")
            if not os.path.exists(merged_capfile_path):
                print(f"!!! {self.__name} - extract Error: cannot fine merged_capfile in Dir: {merged_capfile_dir}")
                result -= 1
                continue

            # 创建overview_extractor的输出目录
            output_dir = os.path.join(storage_Add, "overview_extractor")
            if not os.path.exists(output_dir):
                try:
                    os.makedirs(output_dir, exist_ok=False)
                    print(f"### {self.__name} - extract Info: overview's outputdir:{output_dir} has been generated.")
                except Exception as e:
                    print(f"### {self.__name} - extract Info: overview's outputdir generated Failed for the reason: {e}")
                    result -= 1
                    continue
            # 创建不同分类总览的导出路径
            udp_overview_path = os.path.join(output_dir, "udp_overview.txt")
            tcp_overview_path = os.path.join(output_dir, "tcp_overview.txt")

            # 执行tshark指令工具来导出
            udp_result = self.udp_overview(merged_capfile_path, udp_overview_path)
            tcp_result = self.tcp_overview(merged_capfile_path, tcp_overview_path)
            if udp_result == 0 and tcp_result == 0:
                print(f"### {self.__name} - extract Info: tshark commands execute successfully.")
            else:
                if udp_result == 0:
                    print(f"### {self.__name} - extract Info: udp-overview tshark commands Passed.")
                else:
                    print(f"!!! {self.__name} - extract Warning: udp-overview tshark commands Failed.")
                    result -= 1
                if tcp_result == 0:
                    print(f"### {self.__name} - extract Info: tcp-overview tshark commands Passed.")
                else:
                    print(f"!!! {self.__name} - extract Warning: tcp-overview tshark commands Failed.")
                    result -= 1
        return result
    
    def toString(self):
        return super().toString()