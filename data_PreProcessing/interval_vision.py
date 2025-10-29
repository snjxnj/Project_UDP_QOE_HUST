import re
import os
import pandas as pd
import datetime
import sys
# 设置matplotlib使用非交互式后端，避免弹出窗口
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

timeList_interval_parttern = r'^([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})-([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})$'

def visual_UDP_features(output_path, featuresFile, timeListFile = ''):
    # 初步的可视化，校验文件是否正确
    # print(featuresFile)
    # print(timeListFile)
    # 首先过滤timeListFile当中的双引号
    timeListFile = timeListFile.replace('"', '')
    #然后取出timeListFile路径当中的文件名
    timeListFile_name = timeListFile.split('\\')[-1]
    if not os.path.exists(timeListFile):
        print(f'visual data from featuresFile without timeListFile: {timeListFile_name}')
        # 开始读取featuresFile文件
        features_df = pd.read_csv(featuresFile)
        # 首先将features_df当中的curTime_of_UTC8转换为datatime对象
        features_df['curTime_of_UTC8'] = pd.to_datetime(features_df['curTime_of_UTC8'])
        # 过滤出需要绘制的信息条目
        columns_to_plot = [col for col in features_df.columns if col not in ['curTime_of_UTC8', 'curWindow']]
        # 为每一个绘制信息条目创建一个窗口
        for column in columns_to_plot:
            # 绘制窗口
            plt.figure(figsize=(10,6))
            # 获取当前轴对象
            ax = plt.gca()
            # 绘制指定时间区间的红色背景
            # y_min, y_max = min(features_df[column].min() * 0.9, 0), features_df[column].max() * 1.1
            # for index, row in interval_df.iterrows():
            #     ax.axvspan(row['start_time'], row['end_time'], color='red', alpha=0.3)
            # 绘制折线图
            plt.plot(features_df['curTime_of_UTC8'], features_df[column], linestyle='-', color='blue', label=column)
            plt.title(f'{column}')
            plt.xlabel('time')
            plt.ylabel(f'{column}')
            plt.legend()
            plt.grid(True)
            # 保存图片为PNG格式，使用title作为文件名
            plt.savefig(os.path.join(output_path, f'{column}.png'), dpi=300, bbox_inches='tight')
            # 关闭当前图形，避免内存泄漏
            plt.close()
    else :
        # 构建存储卡顿时间区间的缓存
        timeIntervalList = []
        # 提取timeList表的文件名中的年月日，作为时间信息之一
        parts = timeListFile_name.split("_")
        date = parts[3][:8]
        print(date)
        # 逐行读取timeListFile文件当中的信息
        with open(timeListFile, "r") as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                # print(line)
                if re.match(timeList_interval_parttern, line):
                    # print(line)
                    # 提取时间区间的开始时间和结束时间
                    start_time, end_time = re.match(timeList_interval_parttern, line).groups()
                    # 首先构建开始时间和结束时间的字符串信息
                    start_time_str = f"{date} {start_time}"
                    end_time_str = f"{date} {end_time}"
                    # 转换为datetime对象
                    start_time_dt = datetime.datetime.strptime(start_time_str, "%Y%m%d %H:%M:%S.%f")
                    end_time_dt = datetime.datetime.strptime(end_time_str, "%Y%m%d %H:%M:%S.%f")
                    # 打印以检测提取结果是否正确
                    # print("开始时间:", start_time_dt)
                    # print("结束时间:", end_time_dt)
                    # 将所得的开始时间和终止时间编辑为一条时间区间信息
                    timeInterval = end_time_dt - start_time_dt
                    timeIntervalList.append({
                        "start_time": start_time_dt,
                        "end_time": end_time_dt,
                        "time_interval": timeInterval
                    })
        # 将获取的时间信息整合为df数据结构
        interval_df = pd.DataFrame(timeIntervalList)
        # print(interval_df) # 打印以显示具体信息
        # 开始读取featuresFile文件
        features_df = pd.read_csv(featuresFile)
        # 首先将features_df当中的curTime_of_UTC8转换为datatime对象
        features_df['curTime_of_UTC8'] = pd.to_datetime(features_df['curTime_of_UTC8'])
        # 过滤出需要绘制的信息条目
        columns_to_plot = [col for col in features_df.columns if col not in ['curTime_of_UTC8', 'curWindow']]
        # 为每一个绘制信息条目创建一个窗口
        for column in columns_to_plot:
            # 绘制窗口
            plt.figure(figsize=(10,6))
            # 获取当前轴对象
            ax = plt.gca()
            # 绘制指定时间区间的红色背景
            y_min, y_max = min(features_df[column].min() * 0.9, 0), features_df[column].max() * 1.1
            for index, row in interval_df.iterrows():
                ax.axvspan(row['start_time'], row['end_time'], color='red', alpha=0.3)
            # 绘制折线图
            plt.plot(features_df['curTime_of_UTC8'], features_df[column], linestyle='-', color='blue', label=column)
            plt.title(f'{column}')
            plt.xlabel('time')
            plt.ylabel(f'{column}')
            plt.legend()
            plt.grid(True)
            # 保存图片为PNG格式，使用title作为文件名
            plt.savefig(os.path.join(output_path, f'{column}.png'), dpi=300, bbox_inches='tight')
            # 关闭当前图形，避免内存泄漏
            plt.close()


if __name__ == "__main__":
    # 从命令行参数获取输入
    if len(sys.argv) != 2:
        print("Usage: python interval_vision.py <csvFile_path>")
        sys.exit(1)
    csvFile_path = sys.argv[1]
    control_message = pd.read_csv(csvFile_path)
    print(f'control_message: {control_message.head()}')
    
    # 遍历control_message中的每一行
    for index, row in control_message.iterrows():
        # 首先获取特征文件
        lib_add = row['lib_add']
        # 首先判断lib_add目录中cleaned_data目录是否存在
        if not os.path.exists(os.path.join(lib_add, "cleaned_data")):
            print(f"错误: 目录 '{os.path.join(lib_add, 'cleaned_data')}' 不存在。")
            continue
        featuresFile = os.path.join(lib_add, "cleaned_data", "cleaned_data.csv")
        # print(f"featuresFile: {featuresFile}")
        # 如果lag_timeList_path不是缺省值
        if not pd.isna(row['lag_timeList_path']):
            lag_timeList_path = row['lag_timeList_path']
        else:
            lag_timeList_path = ''
        print(f'lag_timeList_path: {lag_timeList_path}')
        # 检查输出路径lib_add\visualization是否存在，不存在则创建
        if not os.path.exists(os.path.join(lib_add, "visualization")):
            os.makedirs(os.path.join(lib_add, "visualization"))
        visual_UDP_features(output_path = os.path.join(lib_add, "visualization"), featuresFile = featuresFile, timeListFile = lag_timeList_path)
