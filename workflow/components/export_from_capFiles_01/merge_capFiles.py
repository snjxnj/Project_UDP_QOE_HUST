import os
import re
import sys
import pandas as pd
import datetime as dt

"""
@brief 将DF表格中各个样本的cap文件按照时间戳顺序进行合并，导出在存储库中的merged_capFiles目录下
@param df 目标DF表格
@return int值，表示是否成功完成合并
"""
def merge_capFiles(legal_dataFrame_st):
    # 1. 检查legal_dataFrame_st是否为空
    if legal_dataFrame_st.empty:
        print("!!! merge_capFiles Error: The input legal_dataFrame_st is empty. Cannot merge cap files.")
        return -1
    # 2. 检查legal_dataFrame_st是否具有src_Add,scene,ID,storage_Add
    required_columns = ['src_Add', 'scene', 'ID', 'storage_Add']
    for col in required_columns:
        if col not in legal_dataFrame_st.columns:
            print(f"!!! merge_capFiles Error: The required column '{col}' is missing from legal_dataFrame_st. Cannot merge cap files.")
            return -1
    # 3. 遍历legal_dataFrame_st中的各个样本
    for index, row in legal_dataFrame_st.iterrows():
        # 4. 获取当前样本的scene,ID,storage_Add信息
        src_Add = row['src_Add']
        scene = row['scene']
        id = row['ID']
        storage_Add = row['storage_Add']
        # 5. 在src_Add目录下搜索cap文件，目标文件的正则匹配格式为*.pcap*等
        pcap_pattern = re.compile(r'.*\.(cap|pcap|pcapng)(\d)?$', re.IGNORECASE)
        cap_files = []
        for root, dirs, files in os.walk(src_Add):
            # 遍历files列表，找到符合条件的cap文件
            for file in files:
                if pcap_pattern.match(file):
                    cap_files.append(os.path.join(root, file))
        # 如果在src_Add目录下没有找到cap文件，则打印警告信息并继续处理下一个样本记录
        if not cap_files:
            print(f"!!! merge_capFiles Warning: No cap files found in the directory '{src_Add}' for record ID {id}. Skipping merge operation for this record.")
            continue
        print(f"### merge_capFiles Info: Found {len(cap_files)} cap files in the directory '{src_Add}' for record ID {id}. Proceeding with merge operation.")
        # 6. 在storage_Add目录下创建merged_capFiles目录
        merged_cap_dir = os.path.join(storage_Add, "merged_capFiles")
        if not os.path.exists(merged_cap_dir):
            try:
                os.makedirs(merged_cap_dir, exist_ok=False)
                print(f"### merge_capFiles Info: Successfully created directory '{merged_cap_dir}' for merged cap files of record ID {id}.")
            except FileExistsError:
                print(f"!!! merge_capFiles Error: The directory '{merged_cap_dir}' already exists for record ID {id}. Please check the storage directory or remove the existing directory. Skipping merge operation for this record.")
                continue
        # 7. 将cap文件按照时间戳顺序进行合并，导出在merged_capFiles目录下，命名格式为scene_ID_merged.pcap
        # 采取mergecap指令,按照时间戳顺序对cap文件进行合并
        merged_cap_path = os.path.join(merged_cap_dir, f"merged_{scene}_{id}.pcap")
        mergecap_command = f"mergecap -w {merged_cap_path} -a {' '.join(cap_files)}"
        try:
            os.system(mergecap_command)
            # 检查导出目录中的目标文件是否为空，如果为空则说明合并失败，打印错误信息
            if not os.path.exists(merged_cap_path) or os.path.getsize(merged_cap_path) == 0:
                print(f"!!! merge_capFiles Error: The merged cap file '{merged_cap_path}' was not created successfully or is empty for record ID {id}. Please check the mergecap command and the source cap files. Skipping merge operation for this record.")
                continue
            print(f"### merge_capFiles Info: Successfully merged cap files for record ID {id} and exported to '{merged_cap_path}'.")
        except Exception as e:
            print(f"!!! merge_capFiles Error: An error occurred while merging cap files for record ID {id}. Error details: {e}. Skipping merge operation for this record.")
            continue
    print("### merge_capFiles Info: Successfully completed the merging of cap files for all legal records.")
    return 0


if __name__ == "__main__":
    print("### Run merge_capFiles: test")