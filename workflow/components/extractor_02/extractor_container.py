import os
import re
import sys
import pandas as pd
import datetime as dt
from .localIP_extractor import localIP_Extractor
from .udp_extractor import udp_extractor
from .overview_extractor import overview_extractor
from .modem_extractor import modem_extractor
from .setime_extractor import SETime_Extractor

"""
@brief 类extractor_container，目标是作为一个容器类来管理和调用各个Extractor组件进行特征提取
"""
class extractor_container:
    __name = "extractor_container"
    __leagal_dataFrame_st = pd.DataFrame()
    __extractors = []
    __commands = []
    __targetList_path = None

    def __init__(self, leagal_dataFrame_st, commands, targetList_path):
        print(f"### {self.__name} Info: Initializing extractor_container with the provided legal DataFrame with storage information.")
        # 检查输入的leagal_dataFrame_st是否为空
        if leagal_dataFrame_st.empty:
            print(f"!!! {self.__name} Error: The input leagal_dataFrame_st is empty. Cannot initialize extractor_container.")
            return
        # 初始化leagal_dataFrame_st属性
        self.__leagal_dataFrame_st = leagal_dataFrame_st
        # 初始化commands属性
        self.__commands = []
        # 初始化样本信息列表
        self.__targetList_path = targetList_path
        # 开始指令处理
        for command in commands:
            command = command.strip().lower()
            if command == "-u":
                # 检查-u指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化udp_extractor组件添加到extractors列表中
                if not any(cmd.strip().lower() == "-u" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__extractors.append(udp_extractor(self.__leagal_dataFrame_st))
                    print(f"### {self.__name} Info: Added udp_extractor to the list of extractors based on command '-u'.")
                else:
                    print(f"### {self.__name} Info: udp_extractor is already in the list of extractors. Skipping addition based on command '-u'.")
            elif command == "-l":
                # 检查-l指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化localIP_extractor组件添加到extractors列表中
                if not any(cmd.strip().lower() == "-l" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__extractors.append(localIP_Extractor(self.__leagal_dataFrame_st))
                    print(f"### {self.__name} Info: Added localIP_Extractor to the list of extractors based on command '-l'.")
                else:
                    print(f"### {self.__name} Info: localIP_Extractor is already in the list of extractors. Skipping addition based on command '-l'.")
            elif command == "-o":
                # 检查-o指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化overview_extractor组件添加到extractros列表中
                if not any(cmd.strip().lower() == "-o" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__extractors.append(overview_extractor(self.__leagal_dataFrame_st))
                    print(f"### {self.__name} Info: Added overview_extractor to the list of extractors based on command '-o'.")
                else:
                    print(f"### {self.__name} Info: overview_extractor is already in the list of extractors. Skipping addition based on command '-o'.")
            elif command == "-m":
                # 检查-m指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化modem_extractor组件添加到extractros列表中
                if not any(cmd.strip().lower() == "-m" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__extractors.append(modem_extractor(self.__leagal_dataFrame_st, self.__targetList_path))
                    print(f"### {self.__name} Info: Added overview_extractor to the list of extractors based on command '-o'.")
                else:
                    print(f"### {self.__name} Info: overview_extractor is already in the list of extractors. Skipping addition based on command '-o'.")
            elif command == "-se":
                # 检查-se指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化SETime_Extractor组件添加到extractors列表中
                if not any(cmd.strip().lower() == "-se" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__extractors.append(SETime_Extractor(self.__leagal_dataFrame_st))
                    print(f"### {self.__name} Info: Added SETime_Extractor to the list of extractors based on command '-se'.")
                else:
                    print(f"### {self.__name} Info: SETime_Extractor is already in the list of extractors. Skipping addition based on command '-se'.")
            else:
                print(f"!!! {self.__name} Warning: Unrecognized command '{command}' provided. Skipping this command.")
        
        print(f"### {self.__name} Info: Finished processing commands. The following extractors have been initialized: {[type(extractor).__name__ for extractor in self.__extractors]}.")
    
    """
    @brief work方法，容器开始工作，并开始调用各个Extractor组件进行特征提取
    @return int值，表示是否成功完成特征提取
    """
    def work(self):
        print(f"### {self.__name} Info: Starting feature extraction process.")
        # 1. 按照次序，寻找overview_extractor组件，执行总览信息导出
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

        # 2. 查找localIP_Extractor组件，执行本机IP地址的提取操作
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
        
        # 3. 遍历extractors列表，调用特殊组件外的每个Extractor组件的extract方法进行特征提取
        for extractor in self.__extractors:
            if (not isinstance(extractor, overview_extractor)
                and not isinstance(extractor, localIP_Extractor)
                and not isinstance(extractor, SETime_Extractor)):
                print(f"### {self.__name} Info: Calling extract method of {type(extractor).__name__}...")
                extract_result = extractor.extract(self.__targetList_path)
                if extract_result != 0:
                    print(f"!!! {self.__name} Warning: There were issues during the feature extraction process of {type(extractor).__name__}. Please check the logs for details.")
                    print("-"*30)
                else:
                    print(f"### {self.__name} Info: Successfully completed the feature extraction process of {type(extractor).__name__}.")
                    print("-"*30)
        # 4. 最终执行SETime_Extractor组件的extract方法
        setime_extractor = None
        for extractor in self.__extractors:
            if isinstance(extractor, SETime_Extractor):
                setime_extractor = extractor
                break
        if setime_extractor is not None:
            print(f"### {self.__name} Info: Found SETime_Extractor in the list of extractors. Starting SETime extraction process.")
            setime_result = setime_extractor.extract(self.__targetList_path)
            if setime_result != 0:
                print(f"!!! {self.__name} Warning: There were issues during the feature extraction process of SETime_Extractor. Please check the logs for details.")
            else:
                print(f"### {self.__name} Info: Successfully completed the feature extraction process of SETime_Extractor.")
        else:
            print(f"### {self.__name} Info: No SETime_Extractor found in the list of extractors. Skipping SETime extraction process.")

        print(f"### {self.__name} Info: Finished feature extraction process for all extractors.")
        return 0
