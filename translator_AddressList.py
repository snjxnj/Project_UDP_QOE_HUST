import os
import pandas as pd

def translator_AddressList(path_of_AddressList = '.\\address_List.txt'):
    # 判断文件是否存在
    if not os.path.exists(path_of_AddressList):
        print(f"文件 {path_of_AddressList} 不存在")
        return pd.DataFrame()
    # 读取AddressList.txt文件
    with open(path_of_AddressList, 'r') as file:
        lines = file.readlines()
    # print(lines)
    # 存储提取的信息
    data = []
    # 解析每一行，提取字段信息
    for line in lines:
        # 去除行首尾空白字符
        line = line.strip()
        if not line:    # 跳过空行
            continue
        # 按逗号分割行内容，忽略制表符和空格
        parts = [part.replace('\t', '').replace(' ', '').strip() for part in line.split(',')]
        # 初始化字典存储当前行的信息
        info = {}
        # 提取每个字段
        for part in parts:
            # 按冒号分割键值对
            if ':' in part:
                key, value = part.split(':', 1)  # 只分割第一个冒号
                info[key] = value
        # 将提取的信息添加到数据列表
        data.append(info)
    # 构建dataframe并返回
    df = pd.DataFrame(data)
    # 对src_Add当中的内容进行顾虑，过滤所有双引号
    df['src_Add'] = df['src_Add'].str.replace('"', '')
    # print("DataFrame预览:")
    # print(df)
    return df

if __name__ == "__main__":
    translator_AddressList()
