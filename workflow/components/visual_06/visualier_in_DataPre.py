import os
import sys
from turtle import st
import pandas as pd

from visual_06.udp_visual import udp_visualizer

class visualier_in_DataPre:
    __name = "visualier_in_DataPre"
    __legal_dataFrame_st: pd.DataFrame = None
    __commands = None

    def __init__(self, legal_dataFrame_st: pd.DataFrame, commands: list):
        self.__name = "visualier_in_DataPre"
        self.__commands = commands
        if legal_dataFrame_st is not None:
            self.__legal_dataFrame_st = legal_dataFrame_st
        else:
            print(f"!!! {self.__name} -Init Warning: The legal_dataFrame_st is None.")
    
    def export_visual(self):
        result = 0
        # 根据用户判定是否需要进行持久可视化
        if "-v" not in self.__commands:
            print(f"### {self.__name} visual Info: Visualizing is disabled.")
            return result

        # 遍历各个样本信息，完成可视化导出
        for index, row in self.__legal_dataFrame_st.iterrows():
            # 获取样本信息
            id = row["ID"]
            scene = row["scene"]
            storage_Add = row["storage_Add"]
            # 创建可视化的导出目录
            vis_dir = os.path.join(storage_Add, "visual")
            try:
                os.makedirs(vis_dir, exist_ok=True)
            except Exception as e:
                print(f"!!! {self.__name} visual Error: Something Wrong in opt-dir's creating! Exception: {e}")
                result += -1
                continue
            # 按照既定顺序进行可视化内容
            # 1. UDP数据特征的可视化
            if os.path.exists(os.path.join(storage_Add, "udp_extractor")):
                udp_vis_dir = os.path.join(vis_dir, "udp_visual")
                try:
                    os.makedirs(udp_vis_dir, exist_ok=True)
                    udp_vis = udp_visualizer(storage_Add)
                    result += udp_vis.visual_in_files(udp_vis_dir)
                except Exception as e:
                    print(f"!!! {self.__name} visual Error: Something Wrong in udp_vis_dir's creating! Exception: {e}")
                    result += -1
            else:
                print(f"!!! {self.__name} visual Warning: Cannot find any udp_extractor in sample:{id} at: {storage_Add}!")
                result += -1
            # 其他可视化组件
        
        return result
