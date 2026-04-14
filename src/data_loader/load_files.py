import pandas as pd
import numpy as np
import os

import src.data_preprocess.select_tests_vals as get_tvs
from sklearn.preprocessing import StandardScaler,RobustScaler

def create_sequences(X, y, length=20):
	"""为LSTM系列模型创建序列样本

	Args:
		length (int): 序列长度，默认20
		X (list): 数据矩阵
		y (list): 数据标签

	Returns:
		X,y: [(length x N),(length x N),...],[0,1,0,...]
	"""
	X_sequences = []
	y_sequences = []
	
	for i in range(len(X) - length):
		X_sequences.append(X[i:i + length])
		y_sequences.append(y[i + length])
	
	return np.array(X_sequences), np.array(y_sequences)

def load_single_file_for_multiple(file_path, label_column='label'):
	"""_summary_

	Args:
		file_path (_type_): _description_
		label_column (str, optional): _description_. Defaults to 'label'.

	Raises:
		ValueError: _description_

	Returns:
		_type_: _description_
	"""
	# 加载CSV文件
	df = pd.read_csv(file_path)
	
	# 检查是否有标签列
	if label_column not in df.columns:
		print(f"错误: 文件{file_path}中不存在{label_column}列")
		raise ValueError(f"文件{file_path}中不存在{label_column}列")
	
	# 确定特征列 需要排除的.csv列 - 时间戳、label
	exclude_columns = ['timestamp', 'curTime_of_UTC8', 'curWindow', label_column]	# 自行补充
	feature_columns = [col for col in df.columns if col not in exclude_columns]
	print(f"使用的特征列: {feature_columns}")
	print(f"特征列数量: {len(feature_columns)}")

	# 提取特征值和标签
	X = df[feature_columns].values
	y = df[label_column].values

	return X, y, df, feature_columns
	
def load_multiple_files(directory_path, seed, seq_length=20):
	"""加载多个文件，根据文件名前4位数字区分训练集(偶数)和测试集(奇数)"""
	# 检查目录是否存在
	if not os.path.exists(directory_path):
		print(f"错误: 目录{directory_path}不存在")
		raise ValueError(f"目录{directory_path}不存在")
	
	# 获取目录中所有CSV文件
	csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
	
	if not csv_files:
		print(f"错误: 目录{directory_path}中没有找到CSV文件")
		raise ValueError(f"目录{directory_path}中没有找到CSV文件")
	
	# 根据传入种子划分验证集、测试集
	# 返回如[gaming_2026041401.csv,...,...]的格式
	test_list, val_list = get_tvs.select_test_val_from_dir(directory_path, seed=seed)
	print(f'使用验证集{val_list}')
	print(f'使用测试集{test_list}')
		
	# 初始化训练集和测试集
	train_sequences = []
	train_labels = []
	val_sequences = []
	val_labels = []
	test_sequences = []
	test_labels = []
	
	# 第一次遍历所有CSV文件：挑选出所有训练集准备fit
	X_train_raw = []
	file_info = []   # 记录 (file_path, is_test, is_val)
	for csv_file in csv_files:
		file_path = os.path.join(directory_path, csv_file)
		is_test = csv_file in test_list
		is_val = csv_file in val_list
		file_info.append((file_path, is_test, is_val))
		if not is_test and not is_val:
			# 集合所有训练集取特征构造训练集池
			X, _, _, feature_columns= load_single_file_for_multiple(file_path)
			X_train_raw.append(X)

	# 只用训练集拟合标准化器
	if X_train_raw:
		X_train_raw = np.concatenate(X_train_raw, axis=0)
		scaler = StandardScaler()
		scaler.fit(X_train_raw)
		print("标准化器基于训练集拟合完成")
	else:
		raise ValueError("没有训练集数据，无法拟合标准化器")
	
	# 第二次遍历所有CSV文件：Transform所有文件，并构造序列数据
	for file_path, is_test, is_val in file_info:
		X_raw, y, _, _ = load_single_file_for_multiple(file_path)   # 获取原始特征、标签、DataFrame
		X_scaled = scaler.transform(X_raw)            			 # 对所有文件做标准化
		# 构造序列数据
		X_sequences, y_sequences = create_sequences(X=X_scaled,y=y,length=seq_length)
		if is_test:
			# 测试集
			test_sequences.append(X_sequences)
			test_labels.append(y_sequences)
		elif is_val:
			# 验证集
			val_sequences.append(X_sequences)
			val_labels.append(y_sequences)	
		else:
			# 训练集
			train_sequences.append(X_sequences)
			train_labels.append(y_sequences)
	
	# 合并所有训练集和测试集、验证集
	if train_sequences:
		X_train = np.concatenate(train_sequences, axis=0)
		y_train = np.concatenate(train_labels, axis=0)
	else:
		print("警告: 没有找到训练集文件")
		X_train = np.array([])
		y_train = np.array([])
	
	if test_sequences:
		X_test = np.concatenate(test_sequences, axis=0)
		y_test = np.concatenate(test_labels, axis=0)
	else:
		print("警告: 没有找到测试集文件")
		X_test = np.array([])
		y_test = np.array([])

	if val_sequences:
		X_val = np.concatenate(val_sequences, axis=0)
		y_val = np.concatenate(val_labels, axis=0)
	else:
		print("警告: 没有找到验证集文件")
		X_test = np.array([])
		y_test = np.array([])
	
	print(f"总训练集形状: X={X_train.shape}, y={y_train.shape}")
	print(f"总验证集形状: X={X_val.shape}, y={y_val.shape}")
	print(f"总测试集形状: X={X_test.shape}, y={y_test.shape}")
	
	# 确保训练集不为空
	if len(X_train) == 0:
		raise ValueError("没有有效的训练数据，请检查文件命名是否正确")
	else:
		return X_train, y_train, X_val, y_val, X_test, y_test	