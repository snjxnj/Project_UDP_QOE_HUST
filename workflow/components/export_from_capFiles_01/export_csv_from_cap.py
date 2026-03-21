import re
import os
import sys
import pandas as pd
import datetime as dt

"""
@brief 从一个cap文件实例中提取全部字段并导出到cap文件中
@param cap_file_path cap文件路径
@param output_dir_path 输出cap文件的目录路径
@return int值，表示是否成功导出cap文件
"""
def export_allmessage_to_csv(cap_file_path, output_dir_path, str_id = "001"):
    # 1. 检查cap文件路径是否存在，输出csv文件路径的目录是否存在
    if not os.path.exists(cap_file_path):
        print(f"!!! export_allmessage_to_csv Error: The specified cap file '{cap_file_path}' does not exist. CSV export failed.")
        return -1
    if not os.path.exists(os.path.dirname(output_dir_path)):
        print(f"!!! export_allmessage_to_csv Error: The directory for the specified CSV file '{output_dir_path}' does not exist. CSV export failed.")
        return -1
    
    # 2. 获取目标cap文件的文件名
    cap_file_name = os.path.basename(cap_file_path).split('.')[0]  # 去除扩展名
    print(f"### export_allmessage_to_csv Info: The cap file name extracted from the specified path is '{cap_file_name}'.")

    # 3. 从cap文件中提取全部字段信息
    # 调用tshark指令：tshark -r <cap_file_path> -T fields -E separator=, <... other -e options> > <output_csv_path>
    command = (
        f'tshark -r "{cap_file_path}" -T fields -E header=y -E separator=, -E quote=d '
        f'-e frame.time_epoch -e frame.len -e frame.protocols '
        f'-e ip.src -e ip.dst -e ipv6.src -e ipv6.dst '
        f'-e tcp.srcport -e tcp.dstport -e udp.srcport -e udp.dstport '
        f'-e _ws.col.Info '
        f'> "{output_dir_path}/{str_id}_{cap_file_name}.csv"'
    )
    try:
        # print(command)
        os.system(command)
        print(f"### export_allmessage_to_csv Info: Successfully exported all message information from '{cap_file_path}' to CSV file '{output_dir_path}/{str_id}_{cap_file_name}.csv'.")
        return 0
    except Exception as e:
        print(f"!!! export_allmessage_to_csv Error: Failed to export CSV file due to an error. Error details: {e}. CSV export failed.")
        return -1
    

"""
@brief 根据驱动文件，将所有目标样本中cap文件汇总并返回一个sieries
@param_src_dir 目标样本中cap文件所在的目录路径
@return cap_file_series 包含所有目标样本中cap文件路径的Series
"""
def aggregate_cap_files(src_dir):
    # 1. 检查src_dir目录是否存在
    if not os.path.exists(src_dir):
        print(f"!!! aggregate_cap_files Error: The specified source directory '{src_dir}' does not exist. Cannot aggregate cap files.")
        return pd.Series(dtype=str)

    # 2. 目标cap文件的正则匹配.cap*,.pcap*等格式的文件
    pcap_pattern = re.compile(r'.*\.(cap|pcap|pcapng)(\d)?$', re.IGNORECASE)
    cap_file_paths = []
    for root, dirs, files in os.walk(src_dir):
        for file in files:
            if pcap_pattern.match(file):
                cap_file_paths.append(os.path.join(root, file))
    
    if not cap_file_paths:
        print(f"!!! aggregate_cap_files Warning: No cap files found in the specified source directory '{src_dir}'. Returning an empty Series.")
        return pd.Series(dtype=str)
    
    print(f"### aggregate_cap_files Info: Successfully aggregated {len(cap_file_paths)} cap files from the source directory '{src_dir}'.")
    return pd.Series(cap_file_paths)

"""
@brief 根据驱动文件读入的DF，对所有样本的cap文件进行聚合与csv格式导出
@param legal_dataFrame_st 携带存储库信息的合法DF表格
@return 整形值，表示是否成功完成聚合与导出
"""
def aggregate_and_export_csv(legal_dataFrame_st):
    # 1. 检查DF是否为空
    if legal_dataFrame_st.empty:
        print("!!! aggregate_and_export_csv Error: The input legal_dataFrame_st is empty. Cannot aggregate and export CSV files.")
        return -1
    
    # 2. 遍历各样本记录
    for index, row in legal_dataFrame_st.iterrows():
        # 3. 获取样本的源地址和存储库地址
        src_add = row['src_Add']
        storage_add = row['storage_Add']
        # 4. 在源地址中搜索并聚合目标cap文件
        cap_files = aggregate_cap_files(src_add)
        # 5. 如果没有找到cap文件，则打印警告信息并继续处理下一个样本记录
        if cap_files.empty:
            print(f"!!! aggregate_and_export_csv Warning: No cap files found for record ID {row['ID']} in source address '{src_add}'. Skipping CSV export for this record.")
            continue
        # 6. 如果找到cap文件，则在存储目录下创建目录original_csv_files，并在该目录下按照csv格式导出cap文件
        original_csv_dir = os.path.join(storage_add, "original_csvFiles")
        if not os.path.exists(original_csv_dir):
            try:
                os.makedirs(original_csv_dir, exist_ok=False)
                print(f"### aggregate_and_export_csv Info: Successfully created directory '{original_csv_dir}' for original CSV files of record ID {row['ID']}.")
            except FileExistsError:
                print(f"!!! aggregate_and_export_csv Error: The directory '{original_csv_dir}' already exists for record ID {row['ID']}. Please check the storage directory or remove the existing directory. Skipping CSV export for this record.")
                continue
        # 7. 遍历聚合所得的各个cap文件，按照csv格式导出到original_csv_dir目录下
        for idx, cap_file in enumerate(cap_files):
            # 导出编号采取3位数字格式，数字内容为idx+1
            export_result = export_allmessage_to_csv(cap_file_path=cap_file, output_dir_path=original_csv_dir, str_id=f"{idx + 1:03d}")
            if export_result != 0:
                print(f"!!! aggregate_and_export_csv Warning: Failed to export CSV for cap file '{cap_file}' of record ID {row['ID']}. Continuing with the next file.")
            else:
                print(f"### aggregate_and_export_csv Info: Successfully exported CSV for cap file '{cap_file}' of record ID {row['ID']}.")
    
    return 0



if __name__ == "__main__":
    # 1. 测试export_allmessage_to_csv函数
    print("### Run export_allmessage_to_csv: test")
    result = export_allmessage_to_csv(cap_file_path="../../test/test_export_csv/source_cap/tcp_dump_2025_1025_185420.pcap", 
        output_dir_path="../../test/test_export_csv/output_001")
    if result == 0:
        print("### export_allmessage_to_csv test passed successfully.")
    else:
        print("!!! export_allmessage_to_csv test failed.")
    print("---------------------------------")

    # 2. 测试aggregate_cap_files函数
    print("### Run aggregate_cap_files: test")
    cap_files = aggregate_cap_files(src_dir="../../test/test_export_csv/source_cap")
    if not cap_files.empty:
        print(f"### aggregate_cap_files test passed successfully. Aggregated {len(cap_files)} cap files.")
        # 打印聚合的cap文件路径
        for idx, cap_file in enumerate(cap_files):
            print(f"  {idx + 1}. {cap_file}")
    else:
        print("!!! aggregate_cap_files test failed or no cap files found.")
    print("---------------------------------")



