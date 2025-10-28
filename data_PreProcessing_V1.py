"""
2025.8.20
该脚本是DataPreProcessing目录下的顶层脚本，用于调用其他脚本完成整体的数据预处理任务
"""
import sys
import os
import shutil
from datetime import datetime
# 导入其他模块
import translator_AddressList
import sample_Collection_V1
import search_CapFile

def data_PreProcessing_V1():
    # 添加当前目录到Python的搜索路径中
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    # address_List.txt驱动文件的路径
    address_List = ".\\address_List.txt"
    # 获取当前目录地址
    current_path = os.getcwd()
    # 获取格式化的当前系统时间
    now = datetime.now()
    time_str = now.strftime("%Y%m%d_%H%M%S")

    # 调用translate_AddressList函数，以翻译AddressList.txt文件
    sampels_with_IP_df = translator_AddressList.translator_AddressList(address_List)
    if sampels_with_IP_df.empty:
        print("翻译AddressList.txt文件失败")
        return -1
    # print("sampels_with_IP_df:")
    # print(sampels_with_IP_df)
    # print()

    # 调用mkdir_for_samples函数，以创建所有样本的目录
    samples_with_IP_Lib_df = sample_Collection_V1.mkdir_for_samples(sampels_with_IP_df, time_str)
    # print("samples_with_IP_Lib_df:")
    # print(samples_with_IP_Lib_df)
    # print()

    # 调用search_CapFiles函数，以搜索所有样本的Cap文件
    samples_with_IP_Lib_Cap_df = search_CapFile.search_CapFiles(samples_with_IP_Lib_df)
    # print("samples_with_IP_Lib_Cap_df:")
    # print(samples_with_IP_Lib_Cap_df)
    # print()

    csvFile_for_CapOperation = os.path.join(current_path, 'Storage', time_str, "csvFiles_for_CapOperation.csv")
    addressList_copoied = os.path.join(current_path, 'Storage', time_str, "address_List_copied.txt")

    samples_with_IP_Lib_Cap_csvCap_df = samples_with_IP_Lib_Cap_df
    samples_with_IP_Lib_Cap_csvCap_df['csvFiles_for_CapOperation'] = csvFile_for_CapOperation
    samples_with_IP_Lib_Cap_csvCap_df.to_csv(csvFile_for_CapOperation, index=False, encoding='utf-8')

    # 复制address_List.txt到addressList_copoied路径
    shutil.copy2(address_List, addressList_copoied)
    # print(f"已成功将address_List.txt复制到: {addressList_copoied}")

    return csvFile_for_CapOperation

if __name__ == "__main__":
    csvFile_for_CapOperation = data_PreProcessing_V1()
    if csvFile_for_CapOperation == -1:
        print("数据预处理失败")
        exit(-1)
    print(f"已成功将csvFiles_for_CapOperation.csv复制到: {csvFile_for_CapOperation}")
