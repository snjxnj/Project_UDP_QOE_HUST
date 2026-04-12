import pandas as pd
import os
import glob

def check_missing_values(directory):
    """检查目录下所有CSV文件中的缺省值"""
    # 获取所有CSV文件路径
    csv_files = glob.glob(os.path.join(directory, '*.csv'))
    
    if not csv_files:
        print(f"在目录 {directory} 中没有找到CSV文件")
        return
    
    total_files = len(csv_files)
    files_with_missing = 0
    
    print(f"开始检查 {total_files} 个CSV文件...\n")
    
    for file_path in csv_files:
        try:
            # 读取CSV文件
            df = pd.read_csv(file_path)
            
            # 检查是否有缺省值
            missing_count = df.isnull().sum().sum()
            
            if missing_count > 0:
                files_with_missing += 1
                file_name = os.path.basename(file_path)
                
                # 计算每列的缺省值数量
                column_missing = df.isnull().sum()
                columns_with_missing = column_missing[column_missing > 0]
                
                print(f"文件 {file_name} 包含 {missing_count} 个缺省值")
                print("各列缺省值情况:")
                for col, count in columns_with_missing.items():
                    print(f"  - {col}: {count} 个")
                print()
                
        except Exception as e:
            print(f"处理文件 {os.path.basename(file_path)} 时出错: {str(e)}")
    
    print(f"检查完成！")
    print(f"总文件数: {total_files}")
    print(f"包含缺省值的文件数: {files_with_missing}")
    print(f"没有缺省值的文件数: {total_files - files_with_missing}")

if __name__ == "__main__":
    # 指定要检查的目录
    data_directory = "d:\\General_Workspace\\Workspace-of-UDP-NEW\\DataDir\\2025_12_9_FixedDataSheet\\Month10_11\\merged_UDP_Modem\\combine_workspace\\combined_data"
    
    check_missing_values(data_directory)