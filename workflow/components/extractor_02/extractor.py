import os
import re
import sys
import pandas as pd
import datetime as ds
from abc import ABC, abstractmethod

"""
extractor虚拟父类
"""
class Extractor(ABC):
    # 构造函数
    def __init__(self):
        pass

    # 行为规定extract方法，子类必须实现该方法
    @abstractmethod
    def extract(self, *args, **kwargs) -> any:
        pass

    # 行为规定toString方法，子类必须实现该方法
    @abstractmethod
    def toString(self) -> str:
        pass

    # 析构函数
    def __del__(self):
        pass