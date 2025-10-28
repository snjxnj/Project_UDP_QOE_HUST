import os
import re
import sys
import datetime
import pandas as pd

featureFile_pattern = re.compile(r'extracted_([^_]+)_features.csv')

def combine_features(target_files, output_path):
    # 构建合并特征的DataFrame缓存
    combinedData_df = pd.DataFrame()
    # 构建各个文件DataFrame对象集合的列表
    df_list = []
    
    # 遍历所有文件
    for file in target_files:
        print(f"正在处理文件: {file}")
        # 将当前文件的数据特征读取为DataFrame
        df = pd.read_csv(file)
        
        # 将DataFrame当中的‘curTime_of_UTC8’转换为datetime类型
        df['curTime_of_UTC8'] = pd.to_datetime(df['curTime_of_UTC8'])
        
        # 将'curTime_of_UTC8'设置为索引
        df.set_index('curTime_of_UTC8', inplace=True)
        
        # 将当前文件的DataFrame对象加入到列表当中
        df_list.append(df)
    
    # 执行合并操作
    if df_list:
        # 以第一个DataFrame为基础进行合并
        combinedData_df = df_list[0]
        
        # 循环合并后续的DataFrame
        for i in range(1, len(df_list)):
            print(f"正在合并第{i+1}/{len(df_list)}个DataFrame...")
            combinedData_df = pd.merge(combinedData_df, df_list[i], 
                                     left_index=True, right_index=True, how='inner')
        
        # 将时间由索引恢复为列数据
        combinedData_df.reset_index(inplace=True)
        
        # 确保时间列名称正确
        if 'index' in combinedData_df.columns:
            combinedData_df.rename(columns={'index': 'curTime_of_UTC8'}, inplace=True)
        
        # 确保输出目录存在
        if not os.path.exists(os.path.dirname(output_path)):
            print(f"警告: combine_features.py 操作下输出目录 {os.path.dirname(output_path)} 不存在")
            return
        
        # 保存合并后的DataFrame到CSV文件
        combinedData_df.to_csv(os.path.join(output_path, 'combined_all_features.csv'), index=False)
        print(f"特征合并完毕合并完成！结果已保存到: {os.path.join(output_path, 'combined_all_features.csv')}")
        print(f"合并后的DataFrame形状: {combinedData_df.shape}")
    else:
        print("警告: 没有找到可合并的DataFrame")
    
    return combinedData_df


if __name__ == "__main__" :
    # 检查命令行参数是否正确
    if len(sys.argv) != 2:
        print("Usage: python combine_features.py <features_dir>")
        sys.exit(1)
    # 获取命令行参数
    features_dir = sys.argv[1]
    # 检查目录是否存在
    if not os.path.exists(features_dir):
        print(f"Error: Directory {features_dir} does not exist")
        sys.exit(1)
    # 构建待处理文件的缓存列表
    target_files = []
    # 遍历目录下文件
    for root, dirs, files in os.walk(features_dir):
        for f in files:
            if featureFile_pattern.match(f):
                # 提取模式名
                mode_name = featureFile_pattern.match(f).group(1)
                # 构建完整的文件路径
                file_path = os.path.join(root, f)
                print(f"特征文件的来源: {mode_name}, 文件路径: {file_path}")
                # 加入缓存列表
                target_files.append(file_path)
    
    # 检查是否有目标文件
    if not target_files:
        print("错误: 没有找到匹配的特征文件")
        sys.exit(1)
    
    # 调用combine_features函数执行合并操作
    print(f"开始合并 {len(target_files)} 个特征文件...")
    combine_features(target_files, features_dir)
    
