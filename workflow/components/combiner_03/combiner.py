import os
import re
import sys
from abc import ABC, abstractmethod

class combiner(ABC):
    # 构造函数
    def __init__(self):
        pass

    # 抽象方法-规范：combine方法实现
    @abstractmethod
    def combine(self, *args, **kwargs) -> any:
        pass

    # 抽象方法-规范：信息反馈
    @abstractmethod
    def toString(self) -> str:
        pass