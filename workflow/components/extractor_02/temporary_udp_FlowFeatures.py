import os
import re
import sys
import pandas as pd
import datetime as dt

"""
@brief 从一个csv文件实例中提取不同UDP流的特征信息，并分别保存到各自的csv文件中
@param csv_file_path csv文件路径
@param output_dir_path 输出csv文件的目录路径
@return int值，表示是否成功导出csv文件
"""
def export_udp_flow_features(csv_file_path, output_dir_path):
    # 1. 检查csv文件路径是否存在，检查导出目录是否存在
    if not os.path.exists(csv_file_path):
        print(f"!!! export_udp_flow_features Error: The specified csv file '{csv_file_path}' does not exist. CSV export failed.")
        return -1
    if not os.path.exists(output_dir_path):
        print(f"!!! export_udp_flow_features Error: The specified output directory '{output_dir_path}' does not exist. CSV export failed.")
        return -1
    
    # 2. 将目标csv文件读入为DF表格
    try:
        df = pd.read_csv(csv_file_path)
        print(f"### export_udp_flow_features Info: Successfully read the specified csv file '{csv_file_path}' into a DataFrame.")
    except Exception as e:
        print(f"!!! export_udp_flow_features Error: Failed to read the specified csv file '{csv_file_path}' into a DataFrame. Error details: {e}. CSV export failed.")
        return -1

    # 3. 查询DF表格中是否具备UDP协议的相关字段，若没有则无法提取UDP流特征，输出错误信息并返回-1
    required_fields = ['frame.time_epoch', 'frame.len', 'frame.protocols', 'ip.src', 'ip.dst', 'ipv6.src', 'ipv6.dst', 'udp.srcport', 'udp.dstport']
    if not all(field in df.columns for field in required_fields):
        print(f"!!! export_udp_flow_features Error: The specified csv file '{csv_file_path}' does not contain all required fields for UDP flow feature extraction. Required fields are: {required_fields}. CSV export failed.")
        return -1
    
    # 4. 

    return 0


"""
@brief 
"""



if __name__ == "__main__":
    print()