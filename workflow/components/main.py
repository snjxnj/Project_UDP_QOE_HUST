import pandas as pd
import sys
import re
import os
from export_from_capFiles_01 import driverfile_processing
from export_from_capFiles_01 import storage_processing
from export_from_capFiles_01 import export_csv_from_cap
from export_from_capFiles_01 import merge_original_csvFiles
from export_from_capFiles_01 import merge_capFiles
from extractor_02.extractor_container import extractor_container
from combiner_03.combiner_driver import combiner_driver
from label_04.labeler import Labeler


if __name__ == "__main__":
    # 1. 读取最后一个命令参数，作为驱动文件路径
    print("### Main Info: Starting to read driver file...")
    targetList_path = sys.argv[-1] if len(sys.argv) > 1 else "./targetList.txt"
    print(f"### Main Info: Driver file path is set to '{targetList_path}'.")
    print("-"*30 + "\n")    
    
    # 2. 调用read_Driver_File函数读取驱动文件
    print("### Main Info: Calling read_Driver_File function...")
    original_dataFrame = driverfile_processing.read_Driver_File(targetList_path)
    # 检查读取结果
    if original_dataFrame.empty:
        print("!!! Main Error: The driver file could not be read or is empty. Exiting the program.")
        sys.exit(1)
    print("### Main Info: Successfully read the driver file into DataFrame.")
    print(f"### Main Info: The DataFrame has {len(original_dataFrame)} records and {len(original_dataFrame.columns)} columns.")
    print("-"*30 + "\n")    

    # 3. 调用check_Driver_File_Legality函数检查驱动文件的合法性，生成合法的驱动文件DF表格
    print("### Main Info: Calling check_Driver_File_Legality function...")
    legal_dataFrame = driverfile_processing.check_Driver_File_Legality(original_dataFrame)
    # 检查合法性检查结果
    if legal_dataFrame.empty:
        print("!!! Main Warning: No legal records found in the driver file after legality check. The resulting legal DataFrame is empty.")
    else:
        print("### Main Info: Successfully checked the legality of the driver file and generated legal DataFrame.")
        print(f"### Main Info: The legal DataFrame has {len(legal_dataFrame)} records and {len(legal_dataFrame.columns)} columns.")
    print("-"*30 + "\n")    

    # 4. 调用generate_storage函数根据合法的驱动文件DF表格创建存储库，并生成携带存储库信息的合法DF表格
    print("### Main Info: Calling generate_storage function...")
    legal_dataFrame_st = storage_processing.generate_storage(legal_dataFrame, output_dir="../output")
    # 检查存储库生成结果
    if legal_dataFrame_st.empty:
        print("!!! Main Warning: No storage was generated due to empty legal DataFrame or issues with the output directory. The resulting legal DataFrame with storage information is empty.")
    else:
        print("### Main Info: Successfully generated storage for legal records and created legal DataFrame with storage information.")
        print(f"### Main Info: The legal DataFrame with storage information has {len(legal_dataFrame_st)} records and {len(legal_dataFrame_st.columns)} columns.")
    print("-"*30 + "\n")    

    # legal_dataFrame_st数据结构中的信息：ID, src_Add, scene, local_ip, server_ip, start_time, end_time, lag_timeList_path, storage_Add

    # 5. 调用aggregate_and_export_csv函数来获取所有各样本中的cap文件，并导出为csv格式
    print("### Main Info: Calling aggregate_and_export_csv function...")
    export_result = export_csv_from_cap.aggregate_and_export_csv(legal_dataFrame_st)
    if export_result != 0:
        print("!!! Main Warning: There were issues during the aggregation and export of CSV files. Please check the logs for details.")
    else:
        print("### Main Info: Successfully completed the aggregation of cap files and export to CSV format for all legal records.")
    print("-"*30 + "\n")    

    # 6. 针对csv文件的拼接操作，将驱动文件DF表格中各样本的csv文件进行合并
    print("### Main Info: Calling merge_original_csv_files function...")
    merge_result = merge_original_csvFiles.merge_original_csv_files(legal_dataFrame_st)
    if merge_result != 0:
        print("!!! Main Warning: There were issues during the merging of original CSV files. Please check the logs for details.")
    else:
        print("### Main Info: Successfully completed the merging of original CSV files for all legal records.")
    print("-"*30 + "\n")    

    # 7. 将各个样本下的所有cap文件按照时间戳顺序进行合并，导出在存储库中的merged_capFiles目录下
    print("### Main Info: Calling merge_capFiles function...")
    merge_cap_result = merge_capFiles.merge_capFiles(legal_dataFrame_st)
    if merge_cap_result != 0:
        print("!!! Main Warning: There were issues during the merging of cap files. Please check the logs for details.")
    else:
        print("### Main Info: Successfully completed the merging of cap files for all legal records.")
    print("-"*30 + "\n")    

    # 8. 用户输入中，除最后一个指令外，将其他指令封装为一个字串列表，作为后续Extractor组件的输入参数
    print("### Main Info: Getting user's commands.")
    commands = sys.argv[1:-1] if len(sys.argv) > 2 else []
    print(f"### Main Info: The following commands will be passed to the Extractor components for feature extraction: {commands}")
    print("-"*30 + "\n")    

    # 9. 实例化extractor_container组件，传入合法的驱动文件DF表格和指令列表，来管理和调用各个Extractor组件进行特征提取
    print("### Main Info: Initializing extractor_container with the legal DataFrame with storage information and the command list...")
    extractor_manager = extractor_container(legal_dataFrame_st, commands, targetList_path)
    print("### Main Info: Successfully initialized extractor_container and set up the extractors based on the provided commands.")
    print("-"*30 + "\n")    

    # 10. 调用extractor_container的work方法来开始特征提取过程
    print("### Main Info: Calling the work method of extractor_container to start the feature extraction process...")
    extractor_manager.work()
    print("### Main Info: Completed the feature extraction process using extractor_container and its managed Extractor components.")
    print("-"*30 + "\n")    

    # 11. 实例化combiner_driver组件
    print("### Main Info: Initializing combiner_driver with the legal DataFrame with storage information and the command list...")
    combiners = combiner_driver(legal_dataFrame_st, commands)
    print("### Main Info: Successfully initialized combiner_driver and set up the combiners based on the provided commands.")
    print("-"*30 + "\n")

    # 12. 调用combiner_driver的work方法，完成数据特征的合并
    print("### Main Info: Calling the work method of combiner_driver to start the feature combination process...")
    combiners.work()
    print("### Main Info: Completed the feature combination process using combiner_driver and its managed combiners.")
    print("-"*30 + "\n")

    # 13. 实例化labeler组件，并调用label_binary_v1方法，对合并后的特征矩阵进行标签处理
    print("### Main Info: Initializing labeler with the legal DataFrame with storage information and the command list...")
    labeler = Labeler(legal_dataFrame_st)
    labeler.label()
    print("### Main Info: Successfully labeled the combined feature matrix.")
    print("-"*30 + "\n")

    