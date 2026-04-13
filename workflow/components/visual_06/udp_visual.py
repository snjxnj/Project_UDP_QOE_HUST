import os
import sys
import re
import json
import pandas as pd
from datetime import datetime as dt
import matplotlib as mp
import matplotlib.pyplot as plt
import matplotlib.figure as Figure

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from components.utils.read_lagList import read_lagList_v1

def is_directory_empty(directory_path):
    """
    检查目录是否为空（不包含隐藏文件/目录）
    返回: 1 为空, 0 不为空, 其他值非法
    """
    if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
        return -1  # 路径不存在或不是目录，返回0
    
    # 获取目录内容
    entries = os.listdir(directory_path)
    
    # 过滤掉隐藏文件/目录（以 . 开头）
    non_hidden_entries = [entry for entry in entries if not entry.startswith('.')]
    
    # 判断是否为空
    return 1 if len(non_hidden_entries) == 0 else 0

"""
2026.04.08
@brief  UDP的可视化组件，我们的实现目标是：
        为visual.py提供定向的精细窗口内绘图
        为main.py的自动化处理提供持久化的绘图与导出
"""
class udp_visualizer:
    __name = "udp_visualizer"
    __sample_dir = None
    __item_json_path = None
    __lag_path = None
    __state = "Unready"

    def __init__(self, sample_dir):
        # 初始化类名称
        self.__name = "udp_visualizer"
        # 初始化绘制器的状态
        self.__state = "Unready"
        # 判定udp数据目录是否存在
        if os.path.exists(sample_dir):
            print(f"### {self.__name} init Info: Got sample-Dir.")
            self.__sample_dir = sample_dir
            # 判定样本下的json条目信息是否存在
            json_path = os.path.join(self.__sample_dir, "message.json")
            if os.path.exists(json_path):
                print(f"### {self.__name} init Info: Got json-path.")
                self.__item_json_path = json_path
                # 判定条目信息中是否可以解析出关键源文件：卡顿区间列表
                try:
                    with open(json_path, "r", encoding="utf-8") as f:
                        message = json.load(f)
                        if "lag_timeList_path" in message.keys():
                            lag_path = message["lag_timeList_path"]
                            if os.path.exists(lag_path):
                                print(f"### {self.__name} init Info: Got lag_path.")
                                self.__lag_path = lag_path
                            else:
                                print(f"!!! {self.__name} init Error: lag_timeList_path doesnot exist!")
                        else:
                            print(f"!!! {self.__name} init Error: No lag_timeList_path in message.json!")
                except Exception as e:
                    print(f"!!! {self.__name} init Error: Something Error in Reading message.json, E: {e}")
            else:
                print(f"!!! {self.__name} init Error: Cannot find any message.json in sample_dir: {self.__sample_dir}")
        else:
            print(f"!!! {self.__name} init Error: Error UDP-Dir!")
        
        # 完成其他参数初始化，使能绘制器的状态
        self.__state = "Ready"
    

    """
    @brief get_graphs_allflow_with_lag方法，绘制所有流的曲线图，并携带卡顿区间信息
    @param flows_map 流信息，字典数据结构，键值对：流标号，特征文件地址
    @param lag_path 卡顿区间列表的地址
    """
    def get_graphs_allflow_with_lag(self, flows_map: dict[int, str], lag_path: str) -> dict[str, Figure]:
        flowsDF_map = {}            # 存储UDP特征矩阵
        features = []               # 存储待绘制的特征名称
        features_merged_map = {}    # 存储已经完成数据矩阵合并后的map结构
        startTime_alludp = None     # 存储UDP特征下的开始时间，dt对象
        endTime_alludp = None       # 存储UDP特征下的截至时间，dt对象
        lagtime_df = None           # 存储卡顿区间列表中读取的卡顿区间信息，DF对象
        graph_map = {}              # 存储绘制后的图片，键值对：特征名称，图片对象

        # 1. 读入特征文件，统计所有特征文件udp_features.csv当中的特征-列表形式汇总
        # 1.1 根据各个流的特征文件地址读入数据矩阵
        for key, value in flows_map.items():
            # 读取每一个流的UDP特征文件
            udpFeatures_path = os.path.join(value, "udp_features.csv")
            if os.path.exists(udpFeatures_path):
                # 读入数据矩阵
                flowDF = pd.read_csv(udpFeatures_path)
                flowsDF_map[key] = flowDF.copy()
                print(f"### {self.__name} get_graphs_allflow_with_lag Info: Successfully transformed udp_features in flow{key} to DF, size: {flowsDF_map[key].shape}.")
            else:
                print(f"!!! {self.__name} get_graphs_allflow_with_lag Warning: Cannot find any udp_features.csv in flow{key} at: {value}!")
                continue
        # 1.2 统计所有特征矩阵中共有的特征名称，作为待绘制的特征
        # 以特征矩阵字典中的第一个DF列名为基础
        cache_features: set[str] = set(flowsDF_map[0].columns)
        # 遍历特征矩阵字典中的各个矩阵，进行与操作
        for df in flowsDF_map.values():
            cache_features &= set(df.columns)
        features = list(cache_features)
        if len(features) == 0:
            print(f"!!! {self.__name} get_graphs_allflow_with_lag Error: There is no features for visualization!")
            return None
        print(f"### {self.__name} get_graphs_allflow_with_lag Info: {len(features)} features is already for visualization.They are: {features}")
        # 1.3 统计所有特征矩阵中的最大时间边界
        # UDP特征的最大时间边界即flow0总流的时间边界
        startTime_alludp = dt.strptime(flowsDF_map[0]["startTime_of_curWin_UTC8"].iloc[0], 
                                    "%Y-%m-%d %H:%M:%S")
        endTime_alludp = dt.strptime(flowsDF_map[0]["startTime_of_curWin_UTC8"].iloc[-1], 
                                    "%Y-%m-%d %H:%M:%S")

        # 2. 读入卡顿列表，获取卡顿区间的日期、最大时间边界
        # 2.1 读入卡顿列表，将卡顿区间信息存储在DF数据结构中
        lagtime_df = read_lagList_v1(self.__lag_path)
        if len(lagtime_df) == 0:
            print(f"!!! {self.__name} get_graphs_allflow_with_lag Warning: There is no lag in flow in file:{self.__lag_path}!")
        else:
            print(f"### {self.__name} get_graphs_allflow_with_lag Info: There is {len(lagtime_df)} lags in file:{self.__lag_path}.")
        # 2.2 读取第一行的lag_startTime和最后一行的lag_endTime作为卡顿区间的前后截至区间
        startTime_alllag = lagtime_df["lag_startTime"].iloc[0]
        endTime_alllag = lagtime_df["lag_endTime"].iloc[-1]
        
        # 3. 匹配二者是否彼此吻合-是否同处于相同日期，且卡顿的前后边界是否处于特征文件边界以内
        print(f"### {self.__name} get_graphs_allflow_with_lag Info: we got all-UDPs startTime: {startTime_alludp:%Y-%m-%d %H:%M:%S.%f}")
        print(f"### {self.__name} get_graphs_allflow_with_lag Info: we got all-UDPs endTime: {endTime_alludp:%Y-%m-%d %H:%M:%S.%f}")
        print(f"### {self.__name} get_graphs_allflow_with_lag Info: we got all-lags startTime: {startTime_alllag:%Y-%m-%d %H:%M:%S.%f}")
        print(f"### {self.__name} get_graphs_allflow_with_lag Info: we got all-lags endTime: {endTime_alllag:%Y-%m-%d %H:%M:%S.%f}")
        if (startTime_alludp <= startTime_alllag
            and endTime_alludp >= endTime_alllag
        ):
            print(f"### {self.__name} get_graphs_allflow_with_lag Info: Valid time of udp and lag. Prepared for visualization.")
        else:
            print(f"!!! {self.__name} get_graphs_allflow_with_lag Warning: Unfit time of udp and lag. Please check!")
            return None

        # 4. 将待绘制的特征提取出来，并和时间信息构成一个DF数据结构，准备绘制
        # 4.1 遍历待绘制的特征
        for feature in features:
            # 4.2 跳过不必要的时间特征
            if (feature == "startTime_of_curWin_Unix"
                or feature == "startTime_of_curWin_UTC8"
            ):
                continue
            # 4.3将个flow下的同一特征合并在1个DF数据结构中
            # 获取基础组件-总流flow0的time列
            cache_df = flowsDF_map[0]["startTime_of_curWin_UTC8"].copy()
            # 以基础组件为主列，开始从各个flow中获取目标特征列，并完成合并
            for key, value in flowsDF_map.items():
                cache_df = pd.merge(
                    cache_df,
                    value[["startTime_of_curWin_UTC8", feature]].rename(columns={feature:f"flow-{key}"}),
                    on = "startTime_of_curWin_UTC8",
                    how = "outer"
                )
            # 检查导出的DF是否为空
            if len(cache_df) == 0:
                print(f"!!! {self.__name} get_graphs_allflow_with_lag Warning: There is no data in feature:{feature}!")
                continue
            # 4.4 存储合并后的DF对象
            features_merged_map[feature] = cache_df.copy()
        print(f"### {self.__name} get_graphs_allflow_with_lag Info: Successfully merged {len(features_merged_map)} features.")
        
        # 5. 绘制所有图片
        # 5.1 颜色板
        colors = ['blue', 'green', 'yellow', 'magenta', 'gray', 'black']
        # 5.2 遍历各个特征
        for feature_name, feature_df in features_merged_map.items():
            # 5.3 创建画幅，轴对象
            fig, ax = plt.subplots(figsize=(12, 6))
            time_col = pd.to_datetime(feature_df["startTime_of_curWin_UTC8"])
            # 5.4 绘制卡顿区间
            for _, lag_row in lagtime_df.iterrows():
                lag_start = pd.to_datetime(lag_row["lag_startTime"])
                lag_end = pd.to_datetime(lag_row["lag_endTime"])
                ax.axvspan(lag_start, lag_end, alpha=0.3, color='red')
            # 5.5 绘制特征曲线
            color_idx = 0
            for col in feature_df.columns:
                if col == "startTime_of_curWin_UTC8":
                    continue

                if col == "flow-0":
                    ax.plot(time_col, feature_df[col], label=col, color=colors[color_idx % len(colors)], linestyle='-', linewidth=1.5)
                else:
                    ax.plot(time_col, feature_df[col], label=col, color=colors[color_idx % len(colors)], linestyle='-', linewidth=1.0)
                color_idx += 1
            # 5.6 设置轴标签
            ax.set_xlabel('time')
            ax.set_ylabel(feature_name)
            ax.set_title(feature_name)
            ax.legend()
            ax.grid(True)
            fig.autofmt_xdate()
            # 5.7 存储绘制后的图片
            graph_map[feature_name] = fig

        return graph_map
    
    """
    @brief get_graph_oneflow_with_lag方法，绘制总流的曲线图，并携带卡顿区间信息
    @param flows_map 流信息，字典数据结构，键值对：流标号，特征文件地址
    @param lag_path 卡顿区间列表的地址
    """
    def get_graph_oneflow_with_lag(self, flows_map: dict[int, str], lag_path: str) -> dict[str, Figure]:
        flowsDF_map = {}            # 存储UDP特征矩阵
        features = []               # 存储待绘制的特征名称
        features_merged_map = {}    # 存储已经完成数据矩阵合并后的map结构
        startTime_alludp = None     # 存储UDP特征下的开始时间，dt对象
        endTime_alludp = None       # 存储UDP特征下的截至时间，dt对象
        lagtime_df = None           # 存储卡顿区间列表中读取的卡顿区间信息，DF对象
        graph_map = {}              # 存储绘制后的图片，键值对：特征名称，图片对象

        # 1. 读入特征文件，统计所有特征文件udp_features.csv当中的特征-列表形式汇总
        # 1.1 根据各个流的特征文件地址读入数据矩阵
        for key, value in flows_map.items():
            # 读取每一个流的UDP特征文件
            udpFeatures_path = os.path.join(value, "udp_features.csv")
            if os.path.exists(udpFeatures_path):
                # 读入数据矩阵
                flowDF = pd.read_csv(udpFeatures_path)
                flowsDF_map[key] = flowDF.copy()
                print(f"### {self.__name} get_graph_oneflow_with_lag Info: Successfully transformed udp_features in flow{key} to DF, size: {flowsDF_map[key].shape}.")
            else:
                print(f"!!! {self.__name} get_graph_oneflow_with_lag Warning: Cannot find any udp_features.csv in flow{key} at: {value}!")
                continue
        # 1.2 统计所有特征矩阵中共有的特征名称，作为待绘制的特征
        # 以特征矩阵字典中的第一个DF列名为基础
        cache_features: set[str] = set(flowsDF_map[0].columns)
        # 遍历特征矩阵字典中的各个矩阵，进行与操作
        for df in flowsDF_map.values():
            cache_features &= set(df.columns)
        features = list(cache_features)
        if len(features) == 0:
            print(f"!!! {self.__name} get_graph_oneflow_with_lag Error: There is no features for visualization!")
            return None
        print(f"### {self.__name} get_graph_oneflow_with_lag Info: {len(features)} features is already for visualization.They are: {features}")
        # 1.3 统计所有特征矩阵中的最大时间边界
        # UDP特征的最大时间边界即flow0总流的时间边界
        startTime_alludp = dt.strptime(flowsDF_map[0]["startTime_of_curWin_UTC8"].iloc[0], 
                                    "%Y-%m-%d %H:%M:%S")
        endTime_alludp = dt.strptime(flowsDF_map[0]["startTime_of_curWin_UTC8"].iloc[-1], 
                                    "%Y-%m-%d %H:%M:%S")

        # 2. 读入卡顿列表，获取卡顿区间的日期、最大时间边界
        # 2.1 读入卡顿列表，将卡顿区间信息存储在DF数据结构中
        lagtime_df = read_lagList_v1(self.__lag_path)
        if len(lagtime_df) == 0:
            print(f"!!! {self.__name} get_graph_oneflow_with_lag Warning: There is no lag in flow in file:{self.__lag_path}!")
        else:
            print(f"### {self.__name} get_graph_oneflow_with_lag Info: There is {len(lagtime_df)} lags in file:{self.__lag_path}.")
        # 2.2 读取第一行的lag_startTime和最后一行的lag_endTime作为卡顿区间的前后截至区间
        startTime_alllag = lagtime_df["lag_startTime"].iloc[0]
        endTime_alllag = lagtime_df["lag_endTime"].iloc[-1]
        
        # 3. 匹配二者是否彼此吻合-是否同处于相同日期，且卡顿的前后边界是否处于特征文件边界以内
        print(f"### {self.__name} get_graph_oneflow_with_lag Info: we got all-UDPs startTime: {startTime_alludp:%Y-%m-%d %H:%M:%S.%f}")
        print(f"### {self.__name} get_graph_oneflow_with_lag Info: we got all-UDPs endTime: {endTime_alludp:%Y-%m-%d %H:%M:%S.%f}")
        print(f"### {self.__name} get_graph_oneflow_with_lag Info: we got all-lags startTime: {startTime_alllag:%Y-%m-%d %H:%M:%S.%f}")
        print(f"### {self.__name} get_graph_oneflow_with_lag Info: we got all-lags endTime: {endTime_alllag:%Y-%m-%d %H:%M:%S.%f}")
        if (startTime_alludp <= startTime_alllag
            and endTime_alludp >= endTime_alllag
        ):
            print(f"### {self.__name} get_graph_oneflow_with_lag Info: Valid time of udp and lag. Prepared for visualization.")
        else:
            print(f"!!! {self.__name} get_graph_oneflow_with_lag Warning: Unfit time of udp and lag. Please check!")
            return None

        # 4. 将待绘制的特征提取出来，并和时间信息构成一个DF数据结构，准备绘制
        # 4.1 遍历待绘制的特征
        for feature in features:
            # 4.2 跳过不必要的时间特征
            if (feature == "startTime_of_curWin_Unix"
                or feature == "startTime_of_curWin_UTC8"
            ):
                continue
            # 4.3将flow-0下的同一特征合并在1个DF数据结构中
            # 获取基础组件-总流flow0的time列
            cache_df = flowsDF_map[0]["startTime_of_curWin_UTC8"].copy()
            # 以基础组件为主列，开始从flow-0中获取目标特征列，并完成合并
            for key, value in flowsDF_map.items():
                if key == 0:
                    cache_df = pd.merge(
                        cache_df,
                        value[["startTime_of_curWin_UTC8", feature]].rename(columns={feature:f"flow-{key}"}),
                        on = "startTime_of_curWin_UTC8",
                        how = "outer"
                    )
            # 检查导出的DF是否为空
            if len(cache_df) == 0:
                print(f"!!! {self.__name} get_graphs_allflow_with_lag Warning: There is no data in feature:{feature}!")
                continue
            # 4.4 存储合并后的DF对象
            features_merged_map[feature] = cache_df.copy()
        print(f"### {self.__name} get_graphs_allflow_with_lag Info: Successfully merged {len(features_merged_map)} features.")
        
        # 5. 绘制所有图片
        # 5.1 颜色板
        colors = ['blue', 'green', 'yellow', 'magenta', 'gray', 'black']
        # 5.2 遍历各个特征
        for feature_name, feature_df in features_merged_map.items():
            # 5.3 创建画幅，轴对象
            fig, ax = plt.subplots(figsize=(12, 6))
            time_col = pd.to_datetime(feature_df["startTime_of_curWin_UTC8"])
            # 5.4 绘制卡顿区间
            for _, lag_row in lagtime_df.iterrows():
                lag_start = pd.to_datetime(lag_row["lag_startTime"])
                lag_end = pd.to_datetime(lag_row["lag_endTime"])
                ax.axvspan(lag_start, lag_end, alpha=0.3, color='red')
            # 5.5 绘制特征曲线
            color_idx = 0
            for col in feature_df.columns:
                if col == "startTime_of_curWin_UTC8":
                    continue

                if col == "flow-0":
                    ax.plot(time_col, feature_df[col], label=col, color=colors[color_idx % len(colors)], linestyle='-', linewidth=1.5)
                else:
                    ax.plot(time_col, feature_df[col], label=col, color=colors[color_idx % len(colors)], linestyle='-', linewidth=1.0)
                color_idx += 1
            # 5.6 设置轴标签
            ax.set_xlabel('time')
            ax.set_ylabel(feature_name)
            ax.set_title(feature_name)
            ax.legend()
            ax.grid(True)
            fig.autofmt_xdate()
            # 5.7 存储绘制后的图片
            graph_map[feature_name] = fig

        return graph_map

    """
    @brief  visual_in_windows方法，通过窗口而非文件的形式提供可视化结果
            旨在提供高精度的可视化观测
    @return 0表示工作正常，其他值表示工作异常
    """
    def visual_in_windows(self):
        result = 0

        # 1. 首先判定绘图的源文件是否完整存在
        # 1.1 通过状态判定json文件是否具备lag卡顿区间表
        if not self.__state == "Ready":
            print(f"!!! {self.__name} visual_in_windows Error: Something Wrong in module-Init!")
            return -1
        # 1.2 判定样本目录中是否具备UDP特征文件，并汇总信息
        flows_map = {} # 由于UDP数据统计下转为多个流结构，所以绘制时我们需要多流结构的整体信息，信息汇总采取map形式
        udp_dir = os.path.join(self.__sample_dir, "udp_extractor")
        if os.path.exists(udp_dir):
            # 遍历所有文件和目录，统计将被绘制的文件数量与索引信息
            for root, dirs, files in os.walk(self.__sample_dir):
                # 构建匹配目录名称video_20251031050010_flow1的正则表达
                pattern = r"^([a-zA-Z]+)+_(\d+)+_flow(\d+)$"
                # 匹配并收集信息
                if ( re.match(pattern, os.path.basename(root)) is not None # 匹配名称
                    and ("up.csv" in files)
                    and ("down.csv" in files)
                    and ("udp_features.csv" in files)
                ):
                    match = re.match(pattern, os.path.basename(root))
                    flows_map[int(match.group(3))] = root
            print(f"### {self.__name} visual_in_windows Info: Prepared UDP-flows: {flows_map}.")
        else:
            print(f"!!! {self.__name} visual_in_windows Error: There is no effective UDP-Extractor-Dir in sample_dir: {self.__sample_dir}!")
        
        # 2. 确保源文件完备，开始绘制图片
        # 2.1 获取图片
        graph_map = self.get_graphs_allflow_with_lag(flows_map, self.__lag_path)
        # 2.2 按照窗口方式，开始绘制
        if graph_map is None or len(graph_map) == 0:
            print(f"!!! {self.__name} visual_in_windows Warning: No graph to display!")
            return -1
        for feature_name, fig in graph_map.items():
            fig.show()
        
        plt.show()



    """
    @brief  visual_in_files方法，通过文件而非窗口的形式提供可视化结果
            旨在持久化并提供概览
    @return 0表示工作正常，其他值表示工作异常
    """
    def visual_in_files(self, opt_dir: str):
        result = 0

        # 1. 首先判定绘图的源文件是否完整存在
        # 1.1 通过状态判定json文件是否具备lag卡顿区间表
        if not self.__state == "Ready":
            print(f"!!! {self.__name} visual_in_files Error: Something Wrong in module-Init!")
            return -1
        # 1.2 判定样本目录中是否具备UDP特征文件，并汇总信息
        flows_map = {} # 由于UDP数据统计下转为多个流结构，所以绘制时我们需要多流结构的整体信息，信息汇总采取map形式
        udp_dir = os.path.join(self.__sample_dir, "udp_extractor")
        if os.path.exists(udp_dir):
            # 遍历所有文件和目录，统计将被绘制的文件数量与索引信息
            for root, dirs, files in os.walk(self.__sample_dir):
                # 构建匹配目录名称video_20251031050010_flow1的正则表达
                pattern = r"^([a-zA-Z]+)+_(\d+)+_flow(\d+)$"
                # 匹配并收集信息
                if ( re.match(pattern, os.path.basename(root)) is not None # 匹配名称
                    and ("up.csv" in files)
                    and ("down.csv" in files)
                    and ("udp_features.csv" in files)
                ):
                    match = re.match(pattern, os.path.basename(root))
                    flows_map[int(match.group(3))] = root
            print(f"### {self.__name} visual_in_files Info: Prepared UDP-flows: {flows_map}.")
        else:
            print(f"!!! {self.__name} visual_in_files Error: There is no effective UDP-Extractor-Dir in sample_dir: {self.__sample_dir}!")
        
        # 2. 确保源文件完备，开始绘制图片
        # 2.1 获取图片
        graph_map = self.get_graph_oneflow_with_lag(flows_map, self.__lag_path)
        # 2.2 按照图片文件方式导出绘图
        if graph_map is None or len(graph_map) == 0:
            print(f"!!! {self.__name} visual_in_files Warning: No graph to display!")
            return -1
        for feature_name, fig in graph_map.items():
            fig.savefig(os.path.join(opt_dir, feature_name + ".png"))
        


if __name__ == "__main__":
    sample_path = "../../output/flow_20260408_213029/video_20251031050010"
    opt_dir = "../../test/test_for_visualization/opt"
    udp_visualizer = udp_visualizer(sample_path)
    # udp_visualizer.visual_in_windows()
    udp_visualizer.visual_in_files(opt_dir)
        
        
        
    