import os
import re
import sys
import pandas as pd
import datetime as dt
from .localIP_extractor import localIP_Extractor
from .udp_extractor import udp_extractor
from .overview_extractor import overview_extractor

"""
@brief 类extractor_container，目标是作为一个容器类来管理和调用各个Extractor组件进行特征提取
"""
class extractor_container:
    __name = "extractor_container"
    __legal_dataFrame_st = pd.DataFrame()
    __extractors = []
    __commands = []

    def __init__(self, legal_dataFrame_st, commands):
        print(f"### {self.__name} Info: Initializing extractor_container with the provided legal DataFrame with storage information.")
        # 检查输入的legal_dataFrame_st是否为空
        if legal_dataFrame_st.empty:
            print(f"!!! {self.__name} Error: The input legal_dataFrame_st is empty. Cannot initialize extractor_container.")
            return
        # 初始化legal_dataFrame_st属性
        self.__legal_dataFrame_st = legal_dataFrame_st
        # 初始化commands属性
        self.__commands = []
        # 开始指令处理
        for command in commands:
            command = command.strip().lower()
            if command == "-u":
                # 检查-u指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化udp_extractor组件添加到extractors列表中
                if not any(cmd.strip().lower() == "-u" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__extractors.append(udp_extractor(self.__legal_dataFrame_st))
                    print(f"### {self.__name} Info: Added udp_extractor to the list of extractors based on command '-u'.")
                else:
                    print(f"### {self.__name} Info: udp_extractor is already in the list of extractors. Skipping addition based on command '-u'.")
            elif command == "-l":
                # 检查-l指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化localIP_extractor组件添加到extractors列表中
                if not any(cmd.strip().lower() == "-l" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__extractors.append(localIP_Extractor(self.__legal_dataFrame_st))
                    print(f"### {self.__name} Info: Added localIP_Extractor to the list of extractors based on command '-l'.")
                else:
                    print(f"### {self.__name} Info: localIP_Extractor is already in the list of extractors. Skipping addition based on command '-l'.")
            elif command == "-o":
                # 检查-o指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化overview_extractor组件添加到extractros列表中
                if not any(cmd.strip().lower() == "-o" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__extractors.append(overview_extractor(self.__legal_dataFrame_st))
                    print(f"### {self.__name} Info: Added overview_extractor to the list of extractors based on command '-o'.")
                else:
                    print(f"### {self.__name} Info: overview_extractor is already in the list of extractors. Skipping addition based on command '-o'.")
            else:
                print(f"!!! {self.__name} Warning: Unrecognized command '{command}' provided. Skipping this command.")
        
        print(f"### {self.__name} Info: Finished processing commands. The following extractors have been initialized: {[type(extractor).__name__ for extractor in self.__extractors]}.")
    
    """
    @brief work方法，容器开始工作，并开始调用各个Extractor组件进行特征提取
    @return int值，表示是否成功完成特征提取
    """
    def work(self):
        print(f"### {self.__name} Info: Starting feature extraction process.")
        # 按照次序，寻找overview_extractor组件，执行总览信息导出
        overviewer = None
        for extractor in self.__extractors:
            if isinstance(extractor, overview_extractor):
                overviewer = extractor
                break
        if overviewer is not None:
            print(f"### {self.__name} Info: Found overview_extractor in the list of extractors. Starting overview extraction process")
            overviewer_result = overviewer.extract()
            if overviewer_result != 0:
                print(f"!!! {self.__name} Warning: There were issues during the feature extraction process of overview_extractor. Please check the logs for details.")
            else:
                print(f"### {self.__name} Info: Successfully completed the feature extraction process of overview_extractor.")
        else:
            print(f"### {self.__name} Info: No overview_extractor found in the list of extractors. Skipping overview extraction process.")

        # 寻找lovalIPExtractor组件，执行本机IP地址的提取操作
        local_ip_extractor = None
        for extractor in self.__extractors:
            if isinstance(extractor, localIP_Extractor):
                local_ip_extractor = extractor
                break
        if local_ip_extractor is not None:
            print(f"### {self.__name} Info: Found localIP_Extractor in the list of extractors. Starting local IP extraction process.")
            local_ip_result = local_ip_extractor.extract()
            if local_ip_result != 0:
                print(f"!!! {self.__name} Warning: There were issues during the feature extraction process of localIP_Extractor. Please check the logs for details.")
            else:
                print(f"### {self.__name} Info: Successfully completed the feature extraction process of localIP_Extractor.")
        else:
            print(f"### {self.__name} Info: No localIP_Extractor found in the list of extractors. Skipping local IP extraction process.")
        
        # 遍历extractors列表，调用除localIP_Extractor的每个Extractor组件的extract方法进行特征提取
        for extractor in self.__extractors:
            if (not isinstance(extractor, overview_extractor)
                and not isinstance(extractor, localIP_Extractor)):
                print(f"### {self.__name} Info: Calling extract method of {type(extractor).__name__}...")
                extract_result = extractor.extract()
                if extract_result != 0:
                    print(f"!!! {self.__name} Warning: There were issues during the feature extraction process of {type(extractor).__name__}. Please check the logs for details.")
                else:
                    print(f"### {self.__name} Info: Successfully completed the feature extraction process of {type(extractor).__name__}.")
        
        print(f"### {self.__name} Info: Finished feature extraction process for all extractors.")
        return 0
