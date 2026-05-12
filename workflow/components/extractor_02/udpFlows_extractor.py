import sys
import os
import re
import pandas as pd
from datetime import datetime, timedelta, time

"""
@brief  UDP微观流结构的构建和特征提取
@note   该模块的载入必须以UDP特征提取模块作为基本底座，换言之，
        该模块载入前必须确保UDP特征提取模块被载入。
"""
class udpflows_extractor:
    __name = "udpflows_extractor"
    __legal_dataframe_st = pd.DataFrame()

    def __init(self, legal_dataframe_st: pd.DataFrame):
        self.__name = "udpflows_extractor"
        # 判定样本信息DF结构的合法性
        if legal_dataframe_st.empty:
            print(f"!!! {self.__name} - init Error: empty legal_dataframe_st!")
            self.__legal_dataframe_st = pd.DataFrame()
            return
        self.__legal_dataframe_st = legal_dataframe_st
    

    def extract(self):
        ret = 0
        
        print("-"*60)
        print(f"### {self.__name} - extract Info: Start extract procession.")

        # 遍历各个样本，完成流结构特征采集
        for index, row in self.__legal_dataframe_st.iterrows():
            # 获取基本样本信息
            id = row["id"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]
            print(f"Start sample -> name:{scene}-{id}")
            # 判定源文件合法性
            # 1. 检查原始cap文件是否存在
            cap_dir = os.path.join(storage_Add, "merged_capFiles")
            cap_file = os.path.join(cap_dir, f"{scene}_{id}.pcap")
            if not os.path.exists(cap_dir):
                ret -= 1
                print(f"!!! {self.__name} - extract Error: There is no merged_capFiles Dir!")
                continue
            if not os.path.exists(cap_file):
                ret -= 1
                print(f"!!! {self.__name} - extract Error: There is no effective pcapFile in smaple: {scene}-{id}.")
                continue
            # 2. 检查原始csv完整导出文件是否存在
            csv_dir = os.path.join(storage_Add, "mergedCsv_Files")
            csv_file = os.path.join(csv_dir, f"{scene}_{id}.csv")
            if not os.path.exists(csv_dir):
                ret -= 1
                print(f"!!! {self.__name} - extract Error: There is no mergedCsv_Files Dir!")
                continue
            if not os.path.exists(csv_file):
                ret -= 1
                print(f"!!! {self.__name} - extract Error: There is no effective mergedCsvFile in sample: {scene}-{id}.")
                continue
            # 3. 检查原始overview文件是否存在
            overview_dir = os.path.join(storage_Add, "overview_extractor")
            !overview_file = os.path.join(overview_dir, "overview_file.txt")
            if not os.path.exists(overview_dir):
                ret -= 1
                print(f"!!! {self.__name} - extract Error: There is no overview Dir!")
                continue
            if not os.path.exists(overview_file):
                ret -= 1
                print(f"!!! {self.__name} - extract Error: There is no effective overview file in sample: {scene}-{id}")
                continue
            # 4. 检查基座UDP特征目录是否存在
            udp_dir = os.path.join(storage_Add, "udp_extractor")
            if os.path.exists(udp_dir):
                ret -= 1
                print(f"!!! {self.__name} - extract Error: There is no udp_extractor Dir in sample: {scene}-{id}.")
                continue
            # 统计udp_dir下目录数量
            flow_count = 0
            pattern = re.compile(!r"flow\d+")
            for root, files, dirs in os.walk(udp_dir):
                for dir in dirs:
                    if pattern.match(dir):
                        flow_count += 1
            if flow_count == 0:
                ret -= 1
                print(f"!!! {self.__name} - extract Error: There is no effective flows in sample: {scene}-{id}.")
                continue
            

            # 创建导出目录
            opt_dir = os.path.join(storage_Add, "udpFlows_extractor")
            os.makedirs(opt_dir, exist_ok=True)            

            # 开始特征提取
            # 1. 读取overview文件，获取UDP协议总通道下的目标通道
            # 构建DF数据结构存储样本下通道信息

            # 2. 对各个被观测通道进行特征抽取
            for index, row in udp_channels_df.iterrows():
                # 2.1 从csv文件中过滤目标通道下的数据信息
                # 2.2 按照相同的UDP特征提取过程完成流通道下的特征提取
                # 2.3 根据配置内容确定是否对单独通道的上下行特征进行导出
                pass
            # 3. 汇总各个被观测通道，生成目标格式化数据特征

            # 特征持久化导出
            print("="*30)

        print(f"### {self.__name} - extract Info: Finish extract procession.")
        print("-"*60)

        return ret

if __name__ == "__main__":
    print(f"### Test: udpFlows_extractor.py")