import os
import re
import sys
import pandas as pd
import datetime

pattern = re.compile(r'combined_all_features.csv')

def cleanOperation_for_oneSample(src_path, output_path, start_time, end_time):
    # 参数列表的合法性验证
    if not(isinstance(start_time, datetime.datetime)
        and isinstance(end_time, datetime.datetime)):
        print("start_time and end_time must be datetime.datetime type")
        return -1
    #寻找目标文件
    # 读取待清洗的特征文件
    # 首先遍历src_path目录
    targetFile = ''
    for root, dirs, files in os.walk(src_path):
        for file in files:
            if pattern.search(file):
                targetFile = os.path.join(root, file)
                break
    if targetFile == '':
        print("Error: No combined_all_features.csv file found in the source path.")
        return -1
    features_df = pd.read_csv(targetFile)
    # 开始清洗数据
    for index, row in features_df.iterrows():
        curTime_of_UTC8 = datetime.datetime.strptime(row['curTime_of_UTC8'], '%Y-%m-%d %H:%M:%S')
        if(curTime_of_UTC8 < start_time or curTime_of_UTC8 > end_time):
            features_df.drop(index, inplace=True)
    # 清洗完成后，将结果保存到新的文件中
    output_file_path = os.path.join(output_path, "cleaned_data.csv")
    features_df.to_csv(output_file_path, index=False)
    print(f"Cleaned file saved as: {output_file_path}")



if __name__ == '__main__':
    # 从命令行参数获取输入
    if len(sys.argv) != 6:
        print("Usage: python clean_data_operation.py <targetFile> <start_time> <end_time>")
        sys.exit(1)
    src_path = sys.argv[1]
    output_path = sys.argv[2]
    start_time_string = sys.argv[3]
    end_time_string = sys.argv[4]
    id_value = sys.argv[5]

    # 检查输入文件是否存在
    if not os.path.exists(src_path):
        print(f"Error: Input file {src_path} does not exist.")
        sys.exit(1)
    # 检查输出目录是否存在，不存在则创建
    if not os.path.exists(output_path):
        print(f"Output directory {output_path} does not exist. Creating it.")
        os.makedirs(output_path)
    # 检查id_value是否为空
    if id_value == '':
        print("Error: id_value is empty.")
        sys.exit(1)
    # 从ID当中解析采样日期
    date = id_value[:8]
    # 检查日期格式是否正确
    if not re.match(r'\d{8}', date):
        print("Error: id_value date format is incorrect.")
        sys.exit(1)
    # 转换日期格式为YYYY-MM-DD
    date = date[:4] + '-' + date[4:6] + '-' + date[6:]
    # 将日期和起始截至时间拼接为完整时间信息
    start_time_string = date + ' ' + start_time_string
    end_time_string = date + ' ' + end_time_string
    # 转换时间字符串为datetime对象
    start_time = datetime.datetime.strptime(start_time_string, '%Y-%m-%d %H-%M-%S')
    end_time = datetime.datetime.strptime(end_time_string, '%Y-%m-%d %H-%M-%S')
    print(f"start_time: {start_time_string}, end_time: {end_time_string}")
    cleanOperation_for_oneSample(src_path, output_path, start_time, end_time)