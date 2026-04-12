import os
import sys
import re
import pandas as pd
import datetime as dt
from .combiner import  Combiner

class modem_combiner(Combiner):
    __name = "modem_combiner"
    __legal_dataFrame_st = pd.DataFrame()

    def __init__(self, legal_dataFrame_st: pd.DataFrame):
        print(f"### {self.__name} Info: Initializing.")
        self.__name = "modem_combiner"
        self.__legal_dataFrame_st = pd.DataFrame()

        if legal_dataFrame_st.empty:
            print(f"!!! {self.__name} Error: cannt Initializing with empty legal_dataFrame_st!")
            return
        
        self.__legal_dataFrame_st = legal_dataFrame_st

        return
    
    """
    @date   2026.04.07
    @brief  combine方法将Modem数据矩阵向UDP数据矩阵融合，时间戳对齐方式以UDP数据为准
    @return 0表示融合成功，其他值表示融合失败
    @note   这是初代版本的Modem-UDP合并方法，只能合并UDP和Modem信息，无法兼容其他方向的合并
    @note   根据进一步的构想，combine的整体逻辑应该是一个递归逻辑：
            假如有n个方向的数据特征，那么就需要合并n-1次；
            第一次合并时，将第一个方向作为主轴(默认UDP优先级最高，顺位第一)，将第二个方向的数据特征对齐到主轴上；
            往后每次合并，都是将其他数据特征向主轴拼接，直到数据特征合并完毕；
            最终返回合并好的数据矩阵。
    @note   而当涉及到多个样本时，我们则建议使用键值对的map结构来描述，以scene-id为key，以数据矩阵为value；
            并且每一个样本都最好维护一个合并状态机，用于阐明该样本的合并情况：成功合并多少数据矩阵、有哪些矩阵未被合并、矩阵的合并顺序(这对于数据集构建十分重要)等等信息
    """
    def combine(self, *args, **kwargs):
        result = args[0] if args else {}
        
        # 遍历样本信息
        for index, row in self.__legal_dataFrame_st.iterrows():
            # 获取样本的基本信息
            id = row["ID"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]
            
            # 1. 检查仓库目录文件中是否具备源数据modem的输出目录
            modem_dir = os.path.join(storage_Add, "modem_extractor")
            if not os.path.exists(modem_dir):
                print(f"!!! {self.__name} - combine Error: Cannot Find sample:{scene}-{id}'s modem_extractor Dir, you need to use command: \"-m\".")
                result[f"{scene}-{id}"] = pd.DataFrame()
                continue
            # 2. 检查仓库目录文件中是否具备主轴UDP数据的输出目录
            udp_dir = os.path.join(storage_Add, "udp_extractor")
            if not os.path.exists(udp_dir):
                print(f"!!! {self.__name} - combine Warning: Cannot Find sample:{scene}-{id}'s udp_extractor Dir.")
                result[f"{scene}-{id}"] = pd.DataFrame()
                continue

            # 3. 获取进行合并的2个数据矩阵
            # 3.1 读取源数据Modem数据矩阵
            modem_path = os.path.join(modem_dir, "...")
            modem_df = pd.read_csv(modem_path, encoding="utf-8", sep=',')
            if modem_df.empty:
                print(f"!!! {self.__name} - combine Error: Modem features is empty, please check sample {scene}-{id}!")
                result[f"{scene}-{id}"] = pd.DataFrame()
                continue
            # 3.2 读取主轴数据矩阵
            udp_path = os.path.join(udp_dir, f"{scene}_{id}_flow0/udp_features.csv")
            udp_df = pd.read_csv(udp_path, encoding="utf-8", sep=',')
            if udp_df.empty:
                print(f"!!! {self.__name} - combine Error: UDP features is empty, please check sample {scene}-{id}!")
                result[f"{scene}-{id}"] = pd.DataFrame()
                continue
            
            # 4. 将udp_df和modem_df进行匹配融合
            # 4.1 将2个数据矩阵当中时间信息切换为datetime数据结构
            udp_df["startTime_of_curWin_UTC8"] = pd.to_datetime(
                udp_df["startTime_of_curWin_UTC8"],
                format="%Y-%m-%d %H:%M:%S"
            )
            modem_df["timestamp"] = pd.to_datetime(
                modem_df["timestamp"],
                format="%Y-%m-%d-%H-%M-%S"
            )
            udp_df = udp_df.sort_values("startTime_of_curWin_UTC8").reset_index(drop=True)
            modem_df = modem_df.sort_values("timestamp").reset_index(drop=True)
            # 4.2 遍历主轴UDP数据矩阵，开始向其中融合Modem数据信息
            udp_combined = []   # 缓存，UDP数据信息条目
            modem_combined = [] # 缓存，Modem数据信息条目
            for index, row in udp_df.iterrows():
                # 获取当前条目信息的窗口前后边界
                startTime_curWin = row["startTime_of_curWin_UTC8"]
                endTime_curWin = startTime_curWin + dt.timedelta(seconds=1)
                # 搜索modem_df中历史信息
                history_rows_in_modem = modem_df[modem_df["timestamp"] <= endTime_curWin]
                if not history_rows_in_modem.empty:
                    # 搜索历史信息中距离最近的信息
                    history_rows_in_modem = history_rows_in_modem.sort_values("timestamp").reset_index(drop=True)
                    latest_row_in_modem = history_rows_in_modem.iloc[-1]
                    # 将该条UDP条目以及匹配完毕的Modem条目
                    udp_combined.append(row)
                    modem_combined.append(latest_row_in_modem)
            udp_combined_df = pd.DataFrame(udp_combined)
            modem_combined_df = pd.DataFrame(modem_combined)
            
            # 5. 开始执行拼接操作
            print(f"### {self.__name} - combine Info: Now we have filtered UDP:{udp_combined.shape}, and filtered Modem:{modem_combined.shape}")
            # 首先确保长度匹配
            if len(udp_combined) != len(modem_combined):
                print(f"!!! {self.__name} - combine Warning: filtered_UDP and Modem have different length, please check sample:{scene}-{id}!")
                result[f"{scene}-{id}"] = pd.DataFrame()
                continue
            # 过滤2个DF数据结构中不需要的列信息
            udp_combined_df = udp_combined_df
            modem_combined_df = modem_combined_df.drop(columns=["timestamp", "label"])
            # 进行合并
            combined_df = pd.concat(
                [udp_combined_df, modem_combined_df],
                axis=1
            )

            # 6. 完成项目统计
            result[f"{scene}-{id}"] = combined_df

        return result
    
    def toString(self):
        return self.__name