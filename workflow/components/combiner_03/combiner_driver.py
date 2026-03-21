import os
import sys
import pandas as pd
from .combiner import combiner

class combiner_driver:
    __legal_dataFrame_st = pd.DataFrame()
    __commands = []

    """
    @brief 构造方法，获取样本目录、用户指令，完成相应combiner的初始化和调用
    @param leagal_dataFrame_st 由顶层main载入的样本目录
    @param commands 由顶层main载入的用户指令
    @param result int值，用于反馈combine操作是否无误完成
    """
    def __init__(self, leagal_dataFrame_st, commands):
        