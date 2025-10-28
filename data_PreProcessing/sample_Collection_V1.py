"""
2025.8.20
该脚本的目标是：从address-List.txt文件中读取地址和情景，随后判断各个地址的合法性，
然后将所有合法地址和对应情景汇总写入dataFrame结构当中，并返回给顶层脚本
"""
import os
from datetime import datetime
import pandas as pd

def list_directory_contents(directory_path, mod):
    try:
        # 获取目录下的所有文件和子目录
        contents = os.listdir(directory_path)
        
        # 返回正确信息所组成的字典
        return {
            'add': directory_path,
            'mod': mod
        }
    except FileNotFoundError:
        print(f"错误: 目录 '{directory_path}' 不存在。")
        return None
    except PermissionError:
        print(f"错误: 没有权限访问目录 '{directory_path}'。")
        return None
    except Exception as e:
        print(f"访问目录 '{directory_path}' 时发生错误: {e}")
        return None

def read_addresses_and_list_contents_from_txt(filename='address-List.txt'):
    entries = []  # 存储所有条目
    
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()  # 去除行末的换行符
            if line:  # 确保行不为空
                # 分割地址和模式
                parts = line.split('\t')
                if len(parts) >= 2:
                    directory_path = parts[0]  # 第一部分是地址
                    mod = parts[1]  # 第二部分是模式
                    entry = list_directory_contents(directory_path, mod)
                    if entry is not None:
                        entries.append(entry)
                else:
                    # 如果没有制表符分隔，则汇报错误放弃执行
                    print(f"错误: '{line}' 格式错误，缺少模式提醒。")
    
    # 生成DataFrame并返回
    df = pd.DataFrame(entries)
    return df

def read_addresses_and_list_contents(filename='address_List.csv'):
    entries = []
    cache_df = pd.read_csv(filename)

    for index, row in cache_df.iterrows():
        samples_ID = row['samples_ID']
        src_Add = row['src_Add']
        mod = row['mod']
        # 这里的entry直接复制了所有内容，但是需要注意的是，这里原本的意思是对add地址内的内容进行检测
        # 确认了add当中合法，则相关信息写入entry，并载入entries当中
        # 如果add当中非法，则entry将不会写入，同时不会将信息载入entries当中，并且需要给出报警
        entry = {
            'samples_ID': samples_ID,
            'src_Add': src_Add,
            'mod': mod
        }
        entries.append(entry)
    # 生成DataFrame并返回
    df = pd.DataFrame(entries)
    return df

def extract_directory_names(path):
    # 获取最后一个目录名
    last_dir = os.path.basename(path.rstrip('\\/'))
    
    # 获取倒数第二个目录名
    parent_path = os.path.dirname(path.rstrip('\\/'))
    second_last_dir = os.path.basename(parent_path)
    
    return second_last_dir, last_dir

def mkdir_for_samples(samples_df, time_str):
    result = pd.DataFrame()
    cache = []
    # 获取当前目录地址
    current_path = os.getcwd()
    # 记录进一步的工作地址
    work_path = os.path.join(current_path, 'Storage', time_str)

    # 遍历DataFrame的每一行
    for index, row in samples_df.iterrows():
        samples_ID = row['ID']
        src_Add = row['src_Add']
        scene = row['scene']
        local_ip = row['local_ip']
        serv_ip = row['serv_ip']
        start_time = row['start_time']
        end_time = row['end_time']
        if 'lag_timeList_path' in row and pd.notna(row['lag_timeList_path']):
            lag_timeList_path = row['lag_timeList_path']
        else:
            lag_timeList_path = ''
        # 提取目录名
        second_Last_Dir, last_Dir = extract_directory_names(src_Add)
        #过滤second_Last_Dir,last_Dir当中的双引号，防止路径错误
        second_Last_Dir = second_Last_Dir.replace('"', '')
        last_Dir = last_Dir.replace('"', '')
        # 创建目录名
        lib_add = os.path.join(work_path, f"{scene}_{samples_ID}_{last_Dir}")
        # 创建目录
        # print(lib_add)
        os.makedirs(lib_add, exist_ok=True)
        # print(f"已创建目录: {new_dir}")
        # 构建新的条目信息
        new_entry = {
            'ID': samples_ID,
            'src_Add': src_Add,
            'scene': scene,
            'lib_add': lib_add,
            'local_ip': local_ip,
            'serv_ip': serv_ip,
            'start_time': start_time,
            'end_time': end_time,
            'lag_timeList_path': lag_timeList_path,
        }
        cache.append(new_entry)
    result = pd.DataFrame(cache)
    return result



# 调用函数读取地址并列出目录内容
# result_df = read_addresses_and_list_contents()
# print("汇总的DataFrame:")
# print(result_df)