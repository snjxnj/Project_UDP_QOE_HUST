import os
import re
import sys
import pandas as pd
import datetime as dt

"""
@brief 根据驱动文件读入的DF，对各个样本下cap文件导出的csv文件进行合并
@param legal_dataFrame_st 携带存储库信息的合法DF表格
@return 整形值，表示是否成功完成合并操作
"""
def merge_original_csv_files(legal_dataFrame_st):
   # 1. 检查DF是否为空
    if legal_dataFrame_st.empty:
        print("!!! merge_original_csv_files Error: The input legal_dataFrame_st is empty. Cannot merge original CSV files.")
        return -1
    # 2. 遍历各样本记录
    for index, row in legal_dataFrame_st.iterrows():
        # 3. 获取样本的存储库地址、场景、ID信息
        storage_add = row['storage_Add']
        scene = row['scene']
        sample_id = row['ID']
        # 4. 在存储目录下搜索original_csv_files目录
        original_csv_dir = os.path.join(storage_add, "original_csvFiles")
        if not os.path.exists(original_csv_dir):
            print(f"!!! merge_original_csv_files Warning: The directory '{original_csv_dir}' does not exist for record ID {sample_id}. Skipping merge operation for this record.")
            continue
        # 5. 如果original_csv_files目录下存在csv文件
        # 目标csvfiles的正则匹配001_<any contents>.csv等格式的文件
        pattern = re.compile(r'^\d{3}_.+\.csv$', re.IGNORECASE)
        csv_files = [file for file in os.listdir(original_csv_dir) if pattern.match(file)]
        if not csv_files:
            print(f"!!! merge_original_csv_files Warning: No CSV files found in the directory '{original_csv_dir}' for record ID {sample_id}. Skipping merge operation for this record.")
            continue
        # 6. 如果找到csv文件，则首先创建合并后csv文件的导出路径：output_dir/merged_csvFiles
        merged_csvDir = os.path.join(storage_add, "merged_csvFiles")
        if not os.path.exists(merged_csvDir):
            try:
                os.makedirs(merged_csvDir, exist_ok=False)
                print(f"### merge_original_csv_files Info: Successfully created directory '{merged_csvDir}' for merged CSV files of record ID {sample_id}.")
            except FileExistsError:
                print(f"!!! merge_original_csv_files Error: The directory '{merged_csvDir}' already exists for record ID {sample_id}. Please check the storage directory or remove the existing directory. Skipping merge operation for this record.")
                continue
        # 7. 调用merge操作进行合并
        merge(original_csv_dir, merged_csvDir, scene, sample_id)
    return 0


"""
@brief 对当个样本下所有csv原始文件进行合并
@param src_dir 原始csv文件所在的目录路径
@param output_dir 合并后的csv文件导出目录路径
@return 整形值，表示是否成功完成合并操作
""" 
def merge(src_dir, output_dir, scene = "empty", sample_id = 0):
    # 1. 检查src_dir和output_dir目录是否存在
    if not os.path.exists(src_dir):
        print(f"!!! merge Error: The source directory '{src_dir}' does not exist.")
        return -1
    if not os.path.exists(output_dir):
        print(f"!!! merge Error: The output directory '{output_dir}' does not exist.")
        return -1
    # 2. 搜索src_dir目录下的csv文件
    # 给出csv文件的正则匹配
    pattern = re.compile(r'^\d{3}_.+\.csv$', re.IGNORECASE)
    csv_files = [file for file in os.listdir(src_dir) if pattern.match(file)]
    if not csv_files:
        print(f"!!! merge Warning: No CSV files found in the source directory '{src_dir}'. Cannot perform merge operation.")
        return -1
    else:
        print(f"### merge Info: Found {len(csv_files)} CSV files in the source directory '{src_dir}' for merging.")
        print(f"### merge Info: The CSV files to be merged are: {csv_files}.")
    # 3. 如果找到csv文件，则使用pandas库将这些csv文件读入为DataFrame表格，并将这些表格进行合并
    dataFrames = []
    for csv_file in csv_files:
        csv_path = os.path.join(src_dir, csv_file)
        try:
            df = pd.read_csv(csv_path)
            dataFrames.append(df)
            print(f"### merge Info: Successfully read CSV file '{csv_path}' into DataFrame with {len(df)} records and {len(df.columns)} columns.")
        except Exception as e:
            print(f"!!! merge Error: Failed to read CSV file '{csv_path}'. Error details: {e}. Skipping this file.")
    if not dataFrames:
        print(f"!!! merge Warning: No CSV files were successfully read from the source directory '{src_dir}'. Cannot perform merge operation.")
        return -1
    merged_dataFrame = pd.concat(dataFrames, ignore_index=True)
    print(f"### merge Info: Successfully merged {len(dataFrames)} DataFrames into a single DataFrame with {len(merged_dataFrame)} records and {len(merged_dataFrame.columns)} columns.")
    # 4. 将合并后的DF结构按照'frame.time_epoch'字段进行升序排序
    if 'frame.time_epoch' in merged_dataFrame.columns:
        merged_dataFrame.sort_values(by='frame.time_epoch', inplace=True)
        print("### merge Info: Successfully sorted the merged DataFrame by 'frame.time_epoch' in ascending order.")
    else:
        print("!!! merge Warning: The 'frame.time_epoch' field is not present in the merged DataFrame. Skipping sorting operation.")
    # 5. 将合并后的DF结构导出为csv文件到output_dir目录下的merged_{scene}_{ID}.csv
    output_csv_path = os.path.join(output_dir, f"merged_{scene}_{sample_id}.csv")
    try:
        merged_dataFrame.to_csv(output_csv_path, index=False)
        print(f"### merge Info: Successfully exported the merged DataFrame to CSV file '{output_csv_path}'.")
        return 0
    except Exception as e:
        print(f"!!! merge Error: Failed to export the merged DataFrame to CSV file '{output_csv_path}'. Error details: {e}.")
        return -1
    return 0

if __name__ == "__main__":
    print("Run merge: test")
    src_dir = "../../test/test_merge_original_csvFiles/src"
    output_dir = "../../test/test_merge_original_csvFiles/opt"
    merge_result = merge(src_dir, output_dir, "test_scene", 1)
    if merge_result == 0:
        print("### merge test passed successfully.")
    else:
        print("!!! merge test failed.")
    print("---------------------------------")