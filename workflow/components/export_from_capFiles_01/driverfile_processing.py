# 引入项目所需的第三方包
import os
import re
import datetime as dt
import pandas as pd
import numpy as np
#--------------分割线----------------------

'''
@brief 将驱动文件中的目标源读写为DF数据结构并返回，以供后续使用
@param file_path 驱动文件路径
@return 目标源的DF表格
'''
def read_Driver_File(file_path = "../targetList.txt"):
    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"!!! read_Driver_File Error: File '{file_path}' does not exist.")
        return pd.DataFrame()
    print(f"### read_Driver_File Info: Reading driver file from '{file_path}'...")
    
    # 将文件内容转化为DateFrame表格
    # 1. 读取文件内容
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 初始化存储列表
    records_list = []
    
    # 2. 逐行读取文件内容
    for line in lines:
        # 3. 行内容的预处理：去除首尾空白，保留""、''中的空格和tab，过滤其他空格和tab
        line = line.strip()
        if not line:
            continue
        
        # 5. 对分隔内容解析键值对，匹配格式：key:value & key:"value"
        # 使用正则表达式匹配：key可能是字母数字下划线连字符，value可以是带引号或不带引号
        pattern = r'(\w+):(?:"([^"]*)"|([^\s,;]+))'
        matches = re.findall(pattern, line)
        
        # 6. 将键值对收集至字典中，并将字典添加至存储列表中
        record_dict = {}
        for match in matches:
            key = match[0]
            # 如果有引号内的值则使用它，否则使用无引号的值
            value = match[1] if match[1] else match[2]
            record_dict[key] = value
        
        if record_dict:
            records_list.append(record_dict)
    
    # 7. 完成逐行扫描后，检查存储列表中各个字典的key值是否彼此一一对应
    #    如果某一个字典当中含有某项key而其他字典没有，则其他字典补充该key并赋值None
    #    以保证所有字典的key值一致
    if records_list:
        # 收集所有可能的key
        all_keys = set()
        for record in records_list:
            all_keys.update(record.keys())
        
        # 补充缺失的key
        for record in records_list:
            for key in all_keys:
                if key not in record:
                    record[key] = None
    
    # 8. 使用pandas库将存储列表转化为DataFrame表格，并返回该表格
    df = pd.DataFrame(records_list)
    
    print(f"### read_Driver_File Info: Successfully parsed {len(df)} records into DataFrame.")
    return df

'''
@brief 检查读入DF表格的合法性
@param df 目标源的DF表格
@return legal_dataFrame,非法样本将被过滤，缺陷样本将在info中表明缺陷原因
'''
def check_Driver_File_Legality(original_dataFrame):
    # 1. 检查original_dataFrame是否为空，如果为空则返回空的legal_dataFrame
    if original_dataFrame.empty:
        print("!!! check_Driver_File_Legality Error: The original DataFrame is empty. Returning an empty legal DataFrame.")
        return pd.DataFrame()
    # 2. 检查original_dataFrame各行数据
    legal_records = []
    for index, row in original_dataFrame.iterrows():
        info = ""
        # 3. 检查该行数据是否具备ID编号，如果没有则打印警告信息并跳过该行数据
        if 'ID' not in row or pd.isna(row['ID']):
            print(f"!!! check_Driver_File_Legality Error: Row {index} is missing 'ID' field. Skipping this row.")
            continue

        # 4. 检查该行数据是否具备场景信息，如果没有则打印警告信息并跳过该行数据
        if 'scene' not in row or pd.isna(row['scene']):
            print(f"!!! check_Driver_File_Legality Error: Row {index} is missing 'scene' field. Skipping this row.")
            continue

        # 5. 检查该行数据是否具备合法且存在的资源文件路径，如果没有则打印错误信息并跳过该行数据
        if 'src_Add' not in row or pd.isna(row['src_Add']):
            print(f"!!! check_Driver_File_Legality Error: Row {index} is missing 'src_Add' field. Skipping this row.")
            continue
        if not os.path.exists(row['src_Add']):
            print(f"!!! check_Driver_File_Legality Error: Row {index} has an invalid 'src_Add' path '{row['src_Add']}'. Skipping this row.")
            continue

        # 6. 如果该行数据不不具备源、目标IP地址、端口号，则在info信息中表明缺陷原因(IP合法性将未来补充或者在其他代码处验证)
        ipv4_pattern = r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'
        ipv6_pattern = r'^[0-9a-fA-F:]+$'
        port_pattern = r'^\d{1,5}$'
        if 'local_ip' not in row or pd.isna(row['local_ip']):
            print(f"!!! check_Driver_File_Legality Warning: Row {index} is missing 'local_ip' field.")
            info += "Missing local_ip; "
        if 'local_port' not in row or pd.isna(row['local_port']):
            print(f"!!! check_Driver_File_Legality Warning: Row {index} is missing 'local_port' field.")
            info += "Missing local_port; "
        if 'server_ip' not in row or pd.isna(row['server_ip']):
            print(f"!!! check_Driver_File_Legality Warning: Row {index} is missing 'server_ip' field.")
            info += "Missing server_ip; "
        if 'server_port' not in row or pd.isna(row['server_port']):
            print(f"!!! check_Driver_File_Legality Warning: Row {index} is missing 'server_port' field.")
            info += "Missing server_port; "
        
        # 7. 如果该行数据不具备合法的实验开始截至时间，则在info信息中表明缺陷原因
        if 'start_time' not in row or pd.isna(row['start_time']):
            print(f"!!! check_Driver_File_Legality Warning: Row {index} is missing 'start_time' field.")
            info += "Missing start_time; "
        if 'end_time' not in row or pd.isna(row['end_time']):
            print(f"!!! check_Driver_File_Legality Warning: Row {index} is missing 'end_time' field.")
            info += "Missing end_time; "
        # 如果实验截至时间早于实验开始时间，则在info信息中表明缺陷原因
        if ('start_time' in row and 'end_time' in row 
            and not pd.isna(row['start_time']) 
            and not pd.isna(row['end_time'])):
            start_time = dt.datetime.strptime(row['start_time'], "%H-%M-%S")
            end_time = dt.datetime.strptime(row['end_time'], "%H-%M-%S")
            if end_time <= start_time:
                print(f"!!! check_Driver_File_Legality Warning: Row {index} has 'end_time' earlier than 'start_time'.")
                info += "end_time is earlier than start_time; "
        
        # 8. 将该样本信息与info合并，并写入legal_records列表中
        legal_record = row.to_dict()
        legal_record['info'] = info.strip()
        legal_records.append(legal_record)

    # 9. 将合法记录转化为DataFrame并返回
    legal_dataFrame = pd.DataFrame(legal_records)
    return legal_dataFrame

if __name__ == "__main__":
    print("### Running read_Driver_File test...")
    result = read_Driver_File()
    print(result)
    print("------------------------------------------------------")

    print("### Running check_Driver_File_Legality test...")
    result = check_Driver_File_Legality(result)
    print(result)
    print("------------------------------------------------------")