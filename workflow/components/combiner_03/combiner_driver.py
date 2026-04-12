import os
import sys
import pandas as pd
from .combiner import Combiner
from .modem_combiner import modem_combiner

class combiner_driver:
    __name = "combiner_driver"
    __legal_dataFrame_st = pd.DataFrame()
    __combiners = []
    __commands = []
    __num_mainStream = 2

    """
    @brief 构造方法，获取样本目录、用户指令，完成相应combiner的初始化和调用
    @param leagal_dataFrame_st 由顶层main载入的样本目录
    @param commands 由顶层main载入的用户指令
    @param result int值，用于反馈combine操作是否无误完成
    """
    def __init__(self, leagal_dataFrame_st, commands):
        print(f"### {self.__name} Info: Initializing combiner_container with the provided legal DataFrame with storage information.")
        # 检查输入的legal_dataFrame_st是否为空
        if leagal_dataFrame_st.empty:
            print(f"!!! {self.__name} Error: The input legal_dataFrame_st is empty. Cannot initialize extractor_container.")
            return
        # 初始化legal_dataFrame_st属性
        self.__legal_dataFrame_st = leagal_dataFrame_st
        # 初始化commands属性
        self.__commands = []
        # 初始化主流的划分阈值
        self.__num_mainStream = 2
        # 开始指令处理
        for command in commands:
            command = command.strip().lower()
            if command == "-cm":
                # 检查-cm指令是否已经存在于commands列表中，如果不存在则添加到commands列表并实例化modem_combiner组件添加到combiners列表中
                if not any(cmd.strip().lower() == "-cm" for cmd in self.__commands):
                    self.__commands.append(command)
                    self.__combiners.append(modem_combiner(self.__legal_dataFrame_st))
                    print(f"### {self.__name} Info: Added modem_combiner to the list of extractors based on command '-cm'.")
                else:
                    print(f"### {self.__name} Info: modem_combiner is already in the list of extractors. Skipping addition based on command '-cm'.")
            else:
                print(f"!!! {self.__name} Warning: Unrecognized command '{command}' provided. Skipping this command.")
        
        print(f"### {self.__name} Info: Finished processing commands. The following combiners have been initialized: {[type(combiner).__name__ for combiner in self.__combiners]}.")

    """
    2024.04.07
    @brief  work方法，容器开始工作，并开始调用各个Combiner组件进行特征合并
    @return int值，表示是否完成特征合并
    @note   根据04.07的判断，combine方法需要完成如下内容：
            0. 默认融合器，即仅仅抓取宏观UDP信息作为最终结果-贴合旧有设计思路
            1. UDP数据特征的融合：宏观结构与微观结构的构建
            2. Modem数据特征的融合：向UDP主轴的融合
            我们将以上内容划分为2个融合器进行：default_combiner.py, udp_combiner.py, modem_combiner.py
    """
    def work_old(self):
        print(f"### {self.__name} Info: Starting feature combination process.")
        
        # 2026.04.07.临时方案，即如果没有指令中并没有指定combiner的使用，那么该步骤将会默认UDP数据作为最终结果。
        # 等待移植进入udp_combiner.py中
        if len(self.__combiners)==0 :
            print(f"### {self.__name} - work Info: No Command for combining, Starting to copy UDP-Data as the combined-Data.")
            # 遍历所有样本信息，完成目录创建和数据转移
            for index, row in self.__legal_dataFrame_st.iterrows():
                # 获取样本信息
                id = row["ID"]
                scene = row["scene"]
                storage_Add = row["storage_Add"]
                # 锁定UDP数据仓库并检测其合法性
                udp_dir = os.path.join(storage_Add, "udp_extractor")
                totalStream_path = None
                mainStream_path = []
                if os.path.exists(udp_dir):
                    # 从中提取flow0-n，即宏观流、主流
                    for i in range(self.__num_mainStream):
                        path = os.path.join(storage_Add, f"{scene}_{id}_fow{i}")
                        if i == 0:
                            if os.path.exists(path):
                                totalStream_path = path
                            else:
                                print(f"!!! {self.__name} - work Warning: No Legal toatal-Stream Path in UDP-Extractor of sample: {scene}-{id}!")
                        else:
                            if os.path.exists(path):
                                mainStream_path.append(path)
                            else:
                                print(f"!!! {self.__name} - work Warning: Wrong mainStream Path of: {scene}_{id}_fow{i} in sample: {scene}-{id}!")
                    # 获取宏观与主流结构后，进行合并操作
                    
                else:
                    print(f"!!! {self.__name} - work Error: No Command for combining, also no UDP-Data for further Porcession!")
        
        # 构建缓存
        combine_result = {}
        # 遍历combiners列表
        for combiner in self.__combiners:
            print(f"### {self.__name} Info: Calling combine method of {type(combiner).__name__}...")
            # 迭代维护，不断更新合并结果
            combine_result = combiner.combine()
            if combine_result != 0:
                print(f"!!! {self.__name} Warning: There were issues during the feature combination process of {type(combiner).__name__}. Please check the logs for details.")
            else:
                print(f"### {self.__name} Info: Successfully completed the feature combination process of {type(combiner).__name__}.")
        
        # 完成所有combiner的合并操作后，完成持久化输出
        print(combine_result.keys)
        
        print(f"### {self.__name} Info: Finished feature combination process for all combiners.")
        return 0


    """
    2026.04.09
    @brief work方法，将用户指令中的combiner要求付诸实现
    @return 0表示合并成功，其他值表示合并中存在问题
    """
    def work(self):
        print(f"### {self.__name} Info: Starting feature combination process.")
        # 1. 创建必要的缓存内容
        result = 0
        combined_map = {}   # dict[str, pd.DataFrame], 存储legal_dataFrame_st中各个样本的合并结果

        # 2. 执行合并操作
        # 2.1 如果用户没有指定任何combiner的话，将会默认拷贝UDP数据矩阵中的flow0(即汇总流的数据矩阵)作为最终结果
        if len(self.__combiners)==0:
            print(f"### {self.__name} Info: No Command for combining, Starting to copy UDP-Data as the combined-Data.")
            for index, row in self.__legal_dataFrame_st.iterrows():
                # 获取样本信息
                id = row["ID"]
                scene = row["scene"]
                storage_Add = row["storage_Add"]
                # 创建导出目录和导出路径
                opt_dir = os.path.join(storage_Add, "combined_csvFile")
                try:
                    os.makedirs(opt_dir, exist_ok=False)
                except Exception as e:
                    print(f"!!! {self.__name} Warning: Something Wrong happended as Error: {e}!")
                    result -= 1
                    continue
                combined_data_path = os.path.join(opt_dir, "combined_data.csv")
                # 将UDP数据中flow0总流数据矩阵拷贝为最终结果
                udp_dir = os.path.join(storage_Add, "udp_extractor")
                # 检查UDP数据仓库是否存在
                if not os.path.exists(udp_dir):
                    print(f"!!! {self.__name} Warning: No Legal UDP-Extractor Path in sample: {scene}-{id}!")
                    result -= 1
                    continue
                totalStream_path = os.path.join(udp_dir, f"{scene}_{id}_flow0")
                target_udpFeatures_csv = os.path.join(totalStream_path, "udp_features.csv")
                # input(f"检查复制路径是否正确：\n{target_udpFeatures_csv},\n{combined_data_path}\n")
                result = os.system(f'copy "{target_udpFeatures_csv}" "{combined_data_path}"')
                if result != 0:
                    print(f"!!! {self.__name} Warning: There were issues during the copy process of {target_udpFeatures_csv} to {combined_data_path}. Please check the logs for details.")
                    result -= 1
                    continue
        else:
            # 2.2 开始遍历legal_dataFrame_st，完成biners列表，完成合并操作
            for index, row in self.__legal_dataFrame_st.iterrows():
                # 获取样本基本信息
                id = row["ID"]
                scene = row["scene"]
                storage_Add = row["storage_Add"]
                # 创建必要的缓存
                combined_df = pd.DataFrame()
                # 确定合并操作的主轴：flow0-UDP数据矩阵，将其读入作为combined_df的初值
                totalStream_path = os.path.join(storage_Add, "udp_extractor", f"{scene}_{id}_fow0")
                if os.path.exists(totalStream_path):
                    target_udpFeatures_csv = os.path.join(totalStream_path, "udp_features.csv")
                    combined_df = pd.read_csv(target_udpFeatures_csv)
                else:
                    print(f"!!! {self.__name} Error: No Legal total-Stream Path in UDP-Extractor of sample: {scene}-{id}!")
                    result -= 1
                    continue
                # 2.3 从合并器仓库中依次抽取合并器并执行相应的执行任务
                for combiner in self.__combiners:
                    result, combined_df = combiner.combine(combined_df)
                    if result != 0:
                        print(f"!!! {self.__name} work Warning: There were issues during the feature combination process of {type(combiner).__name__}. Please check the logs for details.")
                    if combined_df is None:
                        print(f"!!! {self.__name} work Error: FATAL-ERROR!!!, combine method of {type(combiner).__name__} returned None!")
                        result -= 1
                        sys.pause()
                        sys.exit(1)
                        break
                # 2.4 将合并结果导出
                opt_dir = os.path.join(storage_Add, "combined_csvFile")
                try:
                    os.makedirs(opt_dir, exist_ok=False)
                except Exception as e:
                    print(f"!!! {self.__name} work Warning: Something Wrong happended as Error: {e}!")
                    result -= 1
                    continue
                combined_data_path = os.path.join(opt_dir, "combined_data.csv")
                combined_df.to_csv(combined_data_path, index=False)
                
        pass