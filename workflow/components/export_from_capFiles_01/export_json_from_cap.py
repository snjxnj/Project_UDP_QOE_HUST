import re
import os
import sys
import json
import pandas as pd
import datetime as dt

"""
@brief 从一个cap文件实例中提取全部字段并导出到json文件中
@param cap_file_path cap文件路径
@param output_dir_path 输出json文件的目录路径
@return int值，表示是否成功导出json文件
"""
def export_allmessage_to_json(cap_file_path, output_dir_path):
    # 1. 检查cap文件路径是否存在，输出json文件路径的目录是否存在
    if not os.path.exists(cap_file_path):
        print(f"!!! export_allmessage_to_json Error: The specified cap file '{cap_file_path}' does not exist. JSON export failed.")
        return -1
    if not os.path.exists(os.path.dirname(output_dir_path)):
        print(f"!!! export_allmessage_to_json Error: The directory for the specified JSON file '{output_dir_path}' does not exist. JSON export failed.")
        return -1
    
    # 2. 获取目标cap文件的文件名
    cap_file_name = os.path.basename(cap_file_path)
    print(f"### export_allmessage_to_json Info: The cap file name extracted from the specified path is '{cap_file_name}'.")

    # 3. 从cap文件中提取全部字段信息
    # 调用tshark指令：tshark -r <cap_file_path> -T json > <output_json_path>
    command = f'tshark -r "{cap_file_path}" -T json > "{output_dir_path}/{cap_file_name}.json"'
    try:
        # print(command)
        os.system(command)
        print(f"### export_allmessage_to_json Info: Successfully exported all message information from '{cap_file_path}' to JSON file '{output_dir_path}'.")
        return 0
    except Exception as e:
        print(f"!!! export_allmessage_to_json Error: Failed to export JSON file due to an error. Error details: {e}. JSON export failed.")
        return -1
    


if __name__ == "__main__":
    print("### Run export_allmessage_to_json: test")
    result = export_allmessage_to_json(cap_file_path="../../test/test_export_json/source_cap/tcp_dump_2025_1025_185420.pcap", 
        output_dir_path="../../test/test_export_json/output_001")
    if result == 0:
        print("### export_allmessage_to_json test passed successfully.")
    else:
        print("!!! export_allmessage_to_json test failed.") 
    print("---------------------------------")