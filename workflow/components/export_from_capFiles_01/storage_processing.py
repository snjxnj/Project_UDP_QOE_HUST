import os
import re
import sys
import json
import pandas as pd
import numpy as np
import datetime as dt

'''
@brief 根据合法的驱动文件DF表格，创建存储库
@param legal_dataFrame 合法的驱动文件DF表格
@return legal_dataFrame_st 携带存储库信息的合法DF表格
'''
def generate_storage(legal_dataFrame, output_dir="../../output"):
    # 1. 检查leagal_dataFrame是否为空，output_dir目录是否存在
    if legal_dataFrame.empty:
        print("!!! generate_storage Error: The input legal_dataFrame is empty. No storage will be generated.")
        return legal_dataFrame
    if not os.path.exists(output_dir):
        print("!!! generate_storage Error: The specified output directory does not exist. No storage will be generated.")
        # 询问是否创建该目录
        create_dir = input(f"Do you want to create the directory '{output_dir}'? (y/n): ")
        if create_dir.lower() == 'y':
            try:
                os.makedirs(output_dir, exist_ok=True)
                print(f"### generate_storage Info: Successfully created the directory '{output_dir}'. Storage will be generated within this directory.")
            except Exception as e:
                print(f"!!! generate_storage Error: Failed to create the directory '{output_dir}'. Error details: {e}. No storage will be generated.")
                return legal_dataFrame
        else:
            print("### generate_storage Info: Storage generation has been cancelled by the user.")
            return pd.DataFrame()  # 返回一个空的DataFrame以表示没有生成存储库
    else:
        print(f"### generate_storage Info: The specified output directory '{output_dir}' exists. Storage will be generated within this directory.")

    # 2. 获取系统时间
    current_time = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"### genernate_storage Info: Current system time is {current_time}.")
    top_storage_dir = f"{output_dir}/flow_{current_time}"
    # 3. 创建以系统时间命名的顶层存储目录
    try:
        os.makedirs(top_storage_dir, exist_ok=False)
        print(f"### genernate_storage Info: Successfully created top-level storage directory '{top_storage_dir}'.")
    except FileExistsError:
        print(f"!!! genernate_storage Error: The directory '{top_storage_dir}' already exists. Please check the system time or remove the existing directory. Exiting the program.")
        sys.exit(1)
    # 4. 在顶层存储目录下为每条合法记录创建以ID编号命名的子目录，并在该子目录下创建以ID编号命名的资源文件存储目录
    storage_paths = []
    for index, row in legal_dataFrame.iterrows():
        record_id = row['ID']
        record_scene = row['scene']
        record_storage_dir = os.path.join(top_storage_dir, f"{record_scene}_{record_id}")
        try:
            os.makedirs(record_storage_dir, exist_ok=False)
            print(f"### genernate_storage Info: Successfully created storage directory '{record_storage_dir}' for record ID {record_id}.")
        except FileExistsError:
            print(f"!!! genernate_storage Error: The directory '{record_storage_dir}' already exists for record ID {record_id}. Please check the system time or remove the existing directory. Exiting the program.")
            sys.exit(1)
        storage_paths.append(record_storage_dir)
    # 5. 将存储库信息添加到legal_dataFrame中，生成legal _dataFrame_st表格
    legal_dataFrame_st = legal_dataFrame.copy()
    legal_dataFrame_st['storage_Add'] = storage_paths
    print("### genernate_storage Info: Successfully generated storage paths for all legal records and added them to the DataFrame.")

    # 6. 将完备的legal_dataFrame_st表格以csv文件的形式，持久化输出到top_storage_dir目录中
    legal_dataFrame_st_csv = os.path.join(top_storage_dir, "legal_dataFrame_st.csv")
    legal_dataFrame_st.to_csv(legal_dataFrame_st_csv, index=False)

    # 7. 将各个样本的条目信息输出到各样本仓储目录下
    for index, row in legal_dataFrame_st.iterrows():
        id = row["ID"]
        scene = row["scene"]
        storage_Add = row["storage_Add"]
        # 将该样本的字典信息row，以json形式持久化存储在storage_Add目录下
        json_path = os.path.join(storage_Add, f"message.json")
        row.to_json(json_path, force_ascii = False, indent=4)

    return legal_dataFrame_st

if __name__ == "__main__":
    print("### Running generate_storage test...")
    # 创建一个测试的legal_dataFrame
    test_data = {
        'ID': [1, 2],
        'scene': ['sceneA', 'sceneB'],
        'src_Add': ['path/to/resourceA', 'path/to/resourceB']
    }
    test_legal_dataFrame = pd.DataFrame(test_data)
    # 调用generate_storage函数
    result_dataFrame = generate_storage(test_legal_dataFrame)
    print("### Test Result:")
    print(result_dataFrame)
