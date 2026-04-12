import os
import sys
import pandas as pd
from .extractor import Extractor

# 获取项目根目录（Flow_Analysis）
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# 将 components/lib 添加到 sys.path
lib_dir = os.path.join(project_root, "components", "lib")
sys.path.insert(0, lib_dir)

from lib.src.modem.all_log_to_csv import process_all_log_files
from lib.src.modem.calculate import caculate_all_filter_files

class modem_extractor(Extractor):
    __name = "modem_extractor"
    __legal_dataFrame_st = pd.DataFrame()
    __targetList_path = ""

    def __init__(self, legal_dataFrame_st, targetList_path):
        print(f"### {self.__name}  Info: Initializing modem_extractor with the provided legal DataFrame with storage information.")
        # 初始化legal_dataFrame_st属性
        self.__name = "modem_extractor"
        self.__legal_dataFrame_st = pd.DataFrame()
        self.__targetList_path = targetList_path
        
        # 检查输入的legal_dataFrame_st是否为空
        if legal_dataFrame_st.empty:
            print(f"!!! {self.__name} Error: The input legal_dataFrame_st is empty. Cannot initialize modem_extractor.")
            return
        # 检查输入的targetList_path是否合法
        if not os.path.exists(targetList_path):
            print(f"!!! {self.__name} Error: The input targetList_path is illegal.")
            return
        self.__legal_dataFrame_st = legal_dataFrame_st
        self.__targetList_path = targetList_path
        return

    def modem_data_generator(self, data_dir, targetList_path, opt_dir):
        process_all_log_files(
            data_dir, targetList_path, os.path.join(opt_dir, "csv_from_all_logs")
        )
        caculate_all_filter_files(
            targetList_path,
            source_dir= os.path.join(opt_dir, "filtered_modem_data"),
            output_dir= os.path.join(opt_dir, "caculated_modem_data"),
            cmd = "mean"
        )

    def extract(self, *args, **kwargs):
        result = 0
        # 遍历各个样本
        for index, row in self.__legal_dataFrame_st.iterrows():
            # 获取样本信息
            id = row["ID"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]
            data_dir = row["src_Add"]
            targetList_path = self.__targetList_path
            # 创建样本Modem数据的导出目录
            opt_dir = os.path.join(storage_Add, "modem_extractor")
            try:
                os.makedirs(opt_dir, exist_ok=False)
                print(f"### {self.__name} - extractor Info: Successfully Generating opt_dir for sample: {scene}-{id}'s Modem-Extractor")
            except Exception as e:
                print(f"!!! {self.__name} - extractor Error: Something Error: {e}, happened in dir Generating!")
                result -= 1
                continue
            # 获取函数接口必须的信息
            print(f"### {self.__name} - extractor Info: sample:{scene}-{id} starting Modem-Extraction!")
            self.modem_data_generator(data_dir, targetList_path, opt_dir)
            print(f"### {self.__name} - extractor Info: sample:{scene}-{id} finieshed Modem-Extraction!")

        return result

    def toString(self):
        return self.__name