import pandas as pd
import numpy as np
import os

import src.data_preprocess.select_tests_vals as get_tvs

def create_sequences(X, y, length=20):
	"""
	为LSTM系列模型创建序列样本
	length (int): 序列长度，默认20
	X (list): 数据矩阵
	y (list): 数据标签
	return:X,y: [(length x N),(length x N),...],[0,1,0,...]
	"""
	X_sequences = []
	y_sequences = []
	
	for i in range(len(X) - length):
		X_sequences.append(X[i:i + length])
		y_sequences.append(y[i + length])
	
	return np.array(X_sequences), np.array(y_sequences)

def load_single_file_for_multiple(file_path, model, is_test=False):
	"""
	处理单个文件，传入csv文件的绝对路径，模型，测试集标志
	file_path：单个csv的路径
	model:传入使用模型
	is_test：传入测试集标签，判断当前处理的文件是不是测试集
	return：X, y, df, file_info
	file_info的结构为：{'file_name': file_name, 'timestamps或curTime_of_UTC8': timestamp_list, 'label': y, 'test_lenth': test_lenth}
	"""
	# 加载CSV文件
	df = pd.read_csv(file_path)
	
	# 检查是否有标签列
	if model.label_column not in df.columns:
		print(f"错误: 文件{file_path}中不存在{model.label_column}列")
		raise ValueError(f"文件{file_path}中不存在{model.label_column}列")
	
	# 确定特征列 需要排除的.csv列 - 时间戳、label
	exclude_columns = ['timestamp', 'curTime_of_UTC8', 'curWindow', model.label_column]	# 自行补充
	model.feature_columns = [col for col in df.columns if col not in exclude_columns]

	# 提取特征值和标签
	X = df[model.feature_columns].values
	y = df[model.label_column].values

	file_info = None
	# 如果是测试集，构建测试集文件信息字典
	if is_test:
		# 从 file_path 中提取文件名
		file_name = os.path.basename(file_path)
		# 原始csv的行数
		test_length = len(y)
		# 时间戳列表：先使用 'timestamp'，若不存在则尝试 'curTime_of_UTC8'
		timestamp_col = None
		if 'timestamp' in df.columns:
			timestamp_col = 'timestamp'
		elif 'curTime_of_UTC8' in df.columns:
			timestamp_col = 'curTime_of_UTC8'
		else:
			# 如果没有时间戳列，设为空列表
			timestamp_list = []
		if timestamp_col:
			timestamp_list = df[timestamp_col].tolist()
		else:
			raise ValueError(f"测试集文件 {file_path} 必须包含 'timestamp' 或 'curTime_of_UTC8' 列")
		file_info = {'file_name': file_name, f'{timestamp_col}': timestamp_list, 'label': y, 'test_length': test_length}

	return X, y, df, file_info

def load_multiple_files(directory_path, seed, model):
	"""
	加载所有文件，输入种子ID进行验证集、测试集划分，并构造LSTM需要的序列数据格式
	directory_path：样本文件所在目录
	seed：使用种子
	model：使用的模型，先在主函数中声明定义一个后传进来
	return：直接在model.X_train, y_train, X_val, y_val, X_test, y_test中进行反馈
	"""
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
	test_info = []
	
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
			X, _, _, _ = load_single_file_for_multiple(file_path, model)
			X_train_raw.append(X)
	
	print(f"使用的特征列: {model.feature_columns}")
	print(f"特征列数量: {len(model.feature_columns)}")

	# 只用训练集拟合标准化器
	if X_train_raw:
		X_train_raw = np.concatenate(X_train_raw, axis=0)
		scaler = model.scaler
		scaler.fit(X_train_raw)
		print("标准化器基于训练集拟合完成")
	else:
		raise ValueError("没有训练集数据，无法拟合标准化器")
	
	# 第二次遍历所有CSV文件：Transform所有文件，并构造序列数据
	for file_path, is_test, is_val in file_info:
		X_raw, y, _, test_dict = load_single_file_for_multiple(file_path, model, is_test=is_test) # 获取原始特征、标签、DataFrame
		if test_dict is not None:
			test_info.append(test_dict)
		X_scaled = scaler.transform(X_raw)            			 # 对所有文件做标准化
		# 构造序列数据
		X_sequences, y_sequences = create_sequences(X=X_scaled,y=y,length=model.sequence_length)
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
		# 同时保存测试集详细信息
		model.test_info = test_info
	else:
		print("警告: 没有找到测试集文件")
		X_test = np.array([])
		y_test = np.array([])

	if val_sequences:
		X_val = np.concatenate(val_sequences, axis=0)
		y_val = np.concatenate(val_labels, axis=0)
	else:
		print("警告: 没有找到验证集文件")
		X_val = np.array([])
		y_val = np.array([])
	
	print(f"总训练集形状: X={X_train.shape}, y={y_train.shape}")
	print(f"总验证集形状: X={X_val.shape}, y={y_val.shape}")
	print(f"总测试集形状: X={X_test.shape}, y={y_test.shape}")
	
	# 确保训练集不为空
	if len(X_train) == 0:
		raise ValueError("没有有效的训练数据，请检查文件命名是否正确")
	else:
		model.X_train = X_train
		model.y_train = y_train
		model.X_val = X_val
		model.y_val = y_val
		model.X_test = X_test
		model.y_test = y_test