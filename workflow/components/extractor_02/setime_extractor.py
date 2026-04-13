import os
import sys
import pandas as pd
from datetime import datetime as dt, timedelta

from .extractor import Extractor

"""
@brief 类SETime_Extractor，目标是作为一个Extractor组件来提取Start_Time & End_Time特征
"""
class SETime_Extractor(Extractor):
    __name = "SETime_Extractor"             # 类名
    __legal_dataFrame_st = pd.DataFrame()   # 合法数据框，包含样本信息
    __time_offset = 15                      # 时间偏移量，单位：秒，默认值为15秒

    def __init__(self, legal_dataFrame_st, time_offset=15):
        print(f"### {self.__name} Info: Initializing SETime_Extractor with the provided legal DataFrame with storage information and time_offset={time_offset}.")
        # 初始化私有属性
        self.__name = "SETime_Extractor"
        self.__time_offset = time_offset
        if legal_dataFrame_st.empty:
            print(f"!!! {self.__name} Error: The input legal_dataFrame_st is empty. Cannot initialize SETime_Extractor.")
            return
        # 初始化legal_dataFrame_st属性
        self.__legal_dataFrame_st = legal_dataFrame_st
    
    """
    2026.04.11
    @brief  提取Start_Time & End_Time特征
    @param  targetList_path 目标样本列表路径
    @return 0表示提取成功，1表示提取过程存在错误
    @note   需要注意的是，截至2026.04.11版本，该模块默认从UDP数据中提取样本的开始截至区间
    """
    def extract(self, *args, **kwargs):
        # 初始化结果变量
        result = 0
        # 1. 遍历各个样本信息，开始提取各个样本的Start_Time & End_Time特征
        for index, row in self.__legal_dataFrame_st.iterrows():
            # 获取样本信息
            id = row["ID"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]
            # 2. 检查UDP数据库是否存在
            udp_dir = os.path.join(storage_Add, "udp_extractor")
            if not os.path.exists(udp_dir):
                print(f"!!! {self.__name} Warning: No Legal UDP-Extractor Path in sample: {scene}-{id}!")
                result -= 1
                continue
            totalStream_path = os.path.join(udp_dir, f"{scene}_{id}_flow0")
            if not os.path.exists(totalStream_path):
                print(f"!!! {self.__name} Warning: No Legal total-Stream Path in UDP-Extractor of sample: {scene}-{id}!")
                result -= 1
                continue
            target_udpFeatures_csv = os.path.join(totalStream_path, "udp_features.csv")
            if not os.path.exists(target_udpFeatures_csv):
                print(f"!!! {self.__name} Warning: No Legal udp_features.csv Path in UDP-Extractor of sample: {scene}-{id}!")
                result -= 1
                continue
            # 3. 读取UDP数据中flow0总流数据矩阵
            udp_df = pd.read_csv(target_udpFeatures_csv)
            # 4. 提取Start_Time & End_Time特征
            # 4.1 获取样本中UDP通信业务的起始截至时间(读取为字符串)
            startTime_origin_str = udp_df["startTime_of_curWin_UTC8"].iloc[0]
            endTime_origin_str = udp_df["startTime_of_curWin_UTC8"].iloc[-1]
            # 4.2 转换为datetime对象
            startTime_origin_dt = dt.strptime(startTime_origin_str, "%Y-%m-%d %H:%M:%S")
            endTime_origin_dt = dt.strptime(endTime_origin_str, "%Y-%m-%d %H:%M:%S")
            # 4.3 计算Start_Time & End_Time特征(在已有开始截至时间基础上，各内缩一定时间)
            start_time_str = (startTime_origin_dt + timedelta(seconds=self.__time_offset)).strftime("%Y-%m-%d %H:%M:%S")
            end_time_str = (endTime_origin_dt - timedelta(seconds=self.__time_offset)).strftime("%Y-%m-%d %H:%M:%S")
            # 5. 将开始截至时间导出到指定目录中
            opt_dir = os.path.join(storage_Add, "SETime_extractor")
            try:
                os.makedirs(opt_dir, exist_ok=False)
            except Exception as e:
                print(f"!!! {self.__name} Warning: Something Wrong happended as Error: {e}!")
                result -= 1
                continue
            opt_path = os.path.join(opt_dir, f"{scene}_{id}_SETime.csv")
            with open(opt_path, "w") as f:
                f.write(f"Start_Time,End_Time\n")
                f.write(f"{start_time_str},{end_time_str}\n")
        if result == 0:
            print(f"### {self.__name} Info: Successfully completed the extraction of SETime features for all legal records.")
        return result

    def toString(self):
        return self.__name
