import os
import sys
import re
import pandas as pd
import matplotlib.pyplot as plt
# 将工程目录添加至python环境中
project_root = os.path.abspath(os.path.dirname(__file__), "..", "..")
component_dir = os.path.join(project_root, "components")
sys.path.append(component_dir)
# 包含必要的库
# 这里将会包含可视化中必要的内容


class visualier_in_modelEval:
    self.__name = "visualier_in_modelEval"

    def __init__(self):
        pass

    """
    @brief  模型训练完后的可视化
    @param  test_info  测试集与模型测试结果的信息汇总
    @return result  可视化结果的状态码，0为成功，其他为失败
    @note   test_info  中包含了如下信息：
        - filename : str                        测试集文件名
        - timesstamp/curTime_of_UTC8 : list     测试集的时间序列
        - label : list                          测试集的标签(人工标准答案)
        - test_length : int                     测试集区间长度
        - y_prob : list[float]                  测试结果的回归概率序列
        - y_pred : list[int]                    测试结果的二分类序列
    """
    def visual(self, test_info: pd.DataFrame) -> int:
        result = 0
        
        return result


