import os
import sys
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from components.utils.read_lagList import read_lagList_v1

class Labeler:
    __name = "Labeler"
    __legal_dataFrame_st = pd.DataFrame()

    def __init__(self, legal_dataFrame_st: pd.DataFrame):
        # 初始化私有属性
        self.__name = "Labeler"
        # 检查输入合法性并初始化样本信息
        if legal_dataFrame_st.empty:
            print(f"### {self.__name} init Error: Input legal_dataFrame_st is an empty DataFrame.")
            return
        self.__legal_dataFrame_st = legal_dataFrame_st
    
    """
    @brief  label_binary_v1 根据卡顿区间DF对象，对合并后数据矩阵DF对象添加标签
    @param  lag_timeList_df: 卡顿时间列表DataFrame
    @param  combined_df: 合并后数据矩阵DataFrame
    @param  SETime_df: 开始截至时间文件DataFrame
    @return labeled_df: 完成标签工作的数据矩阵-初步数据集
    @note
            1. label_binary_v1 版本输出的标签为2分类标签，0表示非卡顿区间，1表示卡顿区间
            2. label_binary_v1 版本标签的输出逻辑：
                2.1 合并后特征矩阵中，每一行代表一个采样窗口，每一采样窗口携带采样窗口的起始时刻；
                2.2 每一个卡顿区间包含卡顿起始和截至时刻；
                2.3 如果一个采样窗口中，卡顿区间的占比超过50%，则将该窗口标记为卡顿
            3. label_binary_v1 版本关于输出区间的逻辑：
                3.1 完成标签后，label_binary_v1函数将会过滤SETime_df中的开始截至时间以内的数据矩阵并返回
    """
    def label_binary_v1(self, lag_timeList_df:pd.DataFrame, combined_df:pd.DataFrame, SETime_df:pd.DataFrame) -> pd.DataFrame:
        # 1. 缓存初始化与合法性检测
        labeled_df = combined_df.copy()
        if labeled_df.empty or lag_timeList_df.empty:
            return labeled_df
        # 2. 初始化标签列
        labeled_df["label"] = 0
        # 3. 初始化标签超参数
        window_duration = 1.0
        lag_threshold = 0.5
        # 4. 遍历每个采样窗口，计算卡顿时间占比并进行标签判定
        for idx, row in labeled_df.iterrows():
            # 获取当前窗口的起始时间，并计算结束时间
            window_start = pd.to_datetime(row["startTime_of_curWin_UTC8"])
            window_end = window_start + timedelta(seconds=window_duration)
            # 累加当前窗口与所有卡顿区间的重叠时间
            lag_overlap_time = 0.0
            # 遍历所有卡顿区间，计算与当前窗口的重叠部分
            for _, lag_row in lag_timeList_df.iterrows():
                lag_start = lag_row["lag_startTime"]  # 卡顿区间开始时间
                lag_end = lag_row["lag_endTime"]      # 卡顿区间结束时间
                # 计算重叠区间的起始和结束时间
                # max()取较大值确保重叠起始时间不早于窗口和卡顿区间的开始
                # min()取较小值确保重叠结束时间不晚于窗口和卡顿区间的结束
                overlap_start = max(window_start, lag_start)
                overlap_end = min(window_end, lag_end)
                # 判断是否存在有效重叠（起始时间 < 结束时间表示有交集）
                if overlap_start < overlap_end:
                    # 累加该卡顿区间在当前窗口内的重叠时间
                    lag_overlap_time += (overlap_end - overlap_start).total_seconds()
            # 计算当前窗口的卡顿时间占比
            lag_ratio = lag_overlap_time / window_duration
            # 根据阈值判定并更新标签
            # 若卡顿占比超过设定的阈值（50%），则标记为卡顿
            if lag_ratio > lag_threshold:
                labeled_df.at[idx, "label"] = 1
        
        # 5. 根据SETime_df中的起止时间过滤数据
        # 获取有效时间范围
        if not SETime_df.empty and 'Start_Time' in SETime_df.columns and 'End_Time' in SETime_df.columns:
            valid_start_time = pd.to_datetime(SETime_df['Start_Time'].iloc[0])
            valid_end_time = pd.to_datetime(SETime_df['End_Time'].iloc[0])
            # 将labeled_df的时间列转换为datetime类型，便于比较
            labeled_df['startTime_of_curWin_UTC8'] = pd.to_datetime(labeled_df['startTime_of_curWin_UTC8'])
            # 根据起止时间过滤数据，仅保留时间在[valid_start_time, valid_end_time]范围内的数据
            labeled_df = labeled_df[
                (labeled_df['startTime_of_curWin_UTC8'] >= valid_start_time) & 
                (labeled_df['startTime_of_curWin_UTC8'] <= valid_end_time)
            ]
        
        return labeled_df

    """
    @brief  为合并后的数据添加标签
    @return 0表示标签成功，其他值表示存在错误
    """
    def label(self):
        result = 0
        # 遍历各个样本
        for index, row in self.__legal_dataFrame_st.iterrows():
            # 1. 获取标签操作下所需的样本信息
            # 读取必须具备的样本信息
            id = row["ID"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]
            # 读取可能具备的所需样本信息
            if "lag_timeList_path" in row:
                lag_timeList_path = row["lag_timeList_path"]
            else:
                print(f"!!! {self.__name} label Error: sample: {scene}{id} is missing lag_timeList_path.")
                result -= 1
                continue
            # 2. 根据样本信息获取标签操作下所有的源文件即数据
            # 2.1 检查卡顿时间列表是否存在并获取卡顿信息
            lag_timeList_df = pd.DataFrame() # 缓存卡顿时间列表DataFrame
            if os.path.exists(lag_timeList_path):
                lag_timeList_df = read_lagList_v1(lag_timeList_path)
            else:
                print(f"!!! {self.__name} label Error: sample: {scene}{id}'s lag_timeList_path is not exist: {lag_timeList_path}.")
                result -= 1
                continue
            # 2.2 检查合并后文件是否存在并导入为DF数据对象
            combined_df = pd.DataFrame() # 缓存合并操作后文件DataFrame
            combined_csvFile_path = os.path.join(storage_Add, "combined_csvFile", "combined_data.csv")
            if os.path.exists(combined_csvFile_path):
                combined_df = pd.read_csv(combined_csvFile_path)
            else:
                print(f"!!! {self.__name} label Error: sample: {scene}{id}'s combined_csvFile_path is not exist: {combined_csvFile_path}.")
                result -= 1
                continue
            # 2.3 检查开始截至时间文件是否存在并导入为DF数据对象
            SETime_path = os.path.join(storage_Add, "SETime_extractor", f"{scene}_{id}_SEtime.csv")
            SETime_df = pd.DataFrame() # 缓存开始截至时间文件DataFrame
            if os.path.exists(SETime_path):
                SETime_df = pd.read_csv(SETime_path)
            else:
                print(f"!!! {self.__name} label Error: sample: {scene}{id}'s SETime_path is not exist: {SETime_path}.")
                result -= 1
                continue
            # 3. 根据卡顿区间对合并后数据进行标签
            labeled_df = self.label_binary_v1(lag_timeList_df, combined_df, SETime_df)
            # 4. 保存标签后的数据
            # 4.1 检查标签后的文件是否存在并创建目录
            labeled_csvFile_path = os.path.join(storage_Add, "labeled_csvFile", f"{scene}_{id}_labeled_data.csv")
            if not os.path.exists(os.path.dirname(labeled_csvFile_path)):
                os.makedirs(os.path.dirname(labeled_csvFile_path), exist_ok=True)
            # 4.2 保存标签后的数据
            labeled_df.to_csv(labeled_csvFile_path, index=False)
