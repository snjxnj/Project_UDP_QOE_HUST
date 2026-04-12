import os
import sys
import csv
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report,confusion_matrix, ConfusionMatrixDisplay
import datetime
from contextlib import redirect_stdout
from imblearn.over_sampling import SMOTE

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn.metrics")

OP_TIME = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output_info")

class Tee:
	def __init__(self, *streams):
		self.streams = streams

	def write(self, data):
		for s in self.streams:
			s.write(data)

	def flush(self):
		for s in self.streams:
			s.flush()

def read_tests_column(csv_path: str, column_name: str = 'tests') -> list:
	"""
	读取 CSV 文件中的指定列，该列每行为分号分隔的字符串，
	返回列表的列表，每个子列表对应一行的分割结果。

	:param csv_path: CSV 文件路径
	:param column_name: 列名，默认为 'tests'
	:return: 列表的列表，例如 [['gaming_2025102601', 'gaming_2025102502', ...], ...]
	"""
	# 读取 CSV 文件
	df = pd.read_csv(csv_path)
	
	# 检查列是否存在
	if column_name not in df.columns:
		raise ValueError(f"CSV 文件中不存在列 '{column_name}'")
	
	# 对 tests 列进行分割
	result = []
	for val in df[column_name]:
		if pd.isna(val):          # 处理缺失值
			result.append([])
		else:
			# 按分号分割，并去除可能的前后空格
			items = [item.strip() for item in str(val).split(';')]
			result.append(items)
	return result


class RandomForestModel:
	def __init__(self, random_state=42, n_estimators=100, max_depth=None,
				 min_samples_split=2, min_samples_leaf=1, max_features='sqrt'):
		"""
		初始化随机森林模型
		:param random_state: 随机种子
		:param n_estimators: 树的数量
		:param max_depth: 最大深度，None表示不限制
		:param min_samples_split: 内部节点再划分所需最小样本数
		:param min_samples_leaf: 叶子节点最少样本数
		:param max_features: 每棵树的最大特征数
		"""
		self.random_state = random_state
		self.model = RandomForestClassifier(
			n_estimators=n_estimators,
			max_depth=max_depth,
			min_samples_split=min_samples_split,
			min_samples_leaf=min_samples_leaf,
			max_features=max_features,
			random_state=random_state,
			class_weight='balanced', # 候选class_weight={0:1, 1:10}
			n_jobs=-1  # 使用所有CPU核心 
		)
		self.X_train = None
		self.X_test = None
		self.y_train = None
		self.y_test = None
		self.feature_names = None
		self.is_trained = False

	def load_data(self, file_paths, tests_list, time_col='curTime_of_UTC8', label_col='label'):
		"""
		读取多个CSV文件，根据 tests_list 划分训练集和测试集
		:param file_paths: 所有CSV文件的完整路径列表
		:param tests_list: 测试集文件名基础名列表（如 ['gaming_2026013008_combinedUDPModem_ws.csv', ...]）
		:param time_col: 时间戳列名（会被排除）
		:param label_col: 标签列名
		"""
		train_dfs = []
		test_dfs = []

		# 构建测试集文件名集合（基础名）
		test_basenames = set(tests_list)

		for path in file_paths:
			basename = os.path.basename(path)
			df = pd.read_csv(path)
			if basename in test_basenames:
				test_dfs.append(df)
			else:
				train_dfs.append(df)

		if not train_dfs:
			raise ValueError("没有找到训练集文件，请检查 tests_list 是否包含了所有文件")
		if not test_dfs:
			raise ValueError("没有找到测试集文件，请检查 tests_list 中的文件名是否匹配")

		train_data = pd.concat(train_dfs, ignore_index=True)
		test_data = pd.concat(test_dfs, ignore_index=True)

		# 排除时间戳列和标签列，其余作为特征
		exclude_cols = [time_col, label_col]
		feature_cols = [col for col in train_data.columns if col not in exclude_cols]

		X_train_raw = train_data[feature_cols]
		y_train = train_data[label_col]
		X_test_raw = test_data[feature_cols]
		y_test = test_data[label_col]

		# 缺失值处理：用训练集的均值填充训练集和测试集（避免数据泄露）
		if X_train_raw.isnull().any().any():
			print("训练集检测到缺失值，使用训练集各列均值填充")
			train_means = X_train_raw.mean()
			X_train = X_train_raw.fillna(train_means)
			X_test = X_test_raw.fillna(train_means)
		else:
			X_train = X_train_raw
			X_test = X_test_raw

		# 保存特征名和数据集
		self.feature_names = feature_cols
		self.X_train = X_train
		self.X_test = X_test
		self.y_train = y_train
		self.y_test = y_test

		print(f"训练集大小: {len(X_train)}, 测试集大小: {len(X_test)}")

	def train(self, use_smote=False, smote_random_state=42):
		"""
		使用已划分好的训练集训练模型，可选 SMOTE 过采样
		:param use_smote: 是否对训练集应用 SMOTE
		:param smote_random_state: SMOTE 的随机种子
		"""
		if self.X_train is None or self.y_train is None:
			raise RuntimeError("请先调用 load_data() 加载并划分数据")
		
		X_train = self.X_train
		y_train = self.y_train

		# 应用 SMOTE（仅在训练集上）
		if use_smote:
			# 确保数据为 numpy 数组格式
			if isinstance(X_train, pd.DataFrame):
				X_train_np = X_train.values
			else:
				X_train_np = np.array(X_train)
			y_train_np = np.array(y_train).ravel()

			smote = SMOTE(random_state=42, sampling_strategy=0.5)  # 让少数类变为多数类的50%
			X_resampled, y_resampled = smote.fit_resample(X_train_np, y_train_np)
			
			# 转回 DataFrame（保持特征名）或 numpy 均可
			if isinstance(self.X_train, pd.DataFrame):
				X_train = pd.DataFrame(X_resampled, columns=self.feature_names)
			else:
				X_train = X_resampled
			y_train = y_resampled
			print(f"SMOTE 应用完成，训练集大小从 {len(self.X_train)} 变为 {len(X_train)}")

		print(f"训练集大小: {len(X_train)}, 测试集大小: {len(self.X_test)}")
		print("开始训练随机森林...")
		self.model.fit(X_train, y_train)
		self.is_trained = True
		print("训练完成")

	def evaluate(self, seed=None, prob_threshold=0.5, output_dir='./output'):
		"""
		在测试集上评估模型，输出准确率、AUC、分类报告，并绘制混淆矩阵
		:param seed: 随机种子（仅用于记录，不影响已训练模型）
		:param prob_threshold: 分类阈值，默认0.5
		:param output_dir: 输出目录，用于保存图像
		"""
		if not self.is_trained:
			raise RuntimeError("模型尚未训练，请先调用 train() 方法")

		os.makedirs(output_dir, exist_ok=True)

		# 预测概率和类别
		y_proba = self.model.predict_proba(self.X_test)[:, 1]  # 正类概率
		y_pred = (y_proba >= prob_threshold).astype(int)

		# 计算指标
		accuracy = accuracy_score(self.y_test, y_pred)
		auc = roc_auc_score(self.y_test, y_proba)
		report = classification_report(self.y_test, y_pred, target_names=['0', '1'])
		cm = confusion_matrix(self.y_test, y_pred)

		# 打印结果
		print(f"混淆矩阵：（used seed：{seed if seed is not None else self.random_state}）")
		print(cm)
		tn, fp, fn, tp = cm.ravel()
		rate_0 = tn / (tn + fp) if (tn + fp) > 0 else 0.0
		rate_1 = tp / (tp + fn) if (tp + fn) > 0 else 0.0
		print(f"非卡顿判别: {tn}/({tn}+{fp}) = {rate_0:.4f}")
		print(f"卡顿判别: {tp}/({tp}+{fn}) = {rate_1:.4f}")
		print(f"\n准确率 (Accuracy): {accuracy:.4f}")
		print(f"AUC: {auc:.4f}")
		print("\n分类报告:")
		print(report)

		# 绘制混淆矩阵（单图，不包含loss曲线）
		fig, ax = plt.subplots(figsize=(6, 5))
		disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=['0', '1'])
		disp.plot(cmap=plt.cm.Blues, ax=ax, colorbar=False)
		ax.set_title(f"Confusion Matrix (threshold={prob_threshold})")

		plt.tight_layout()

		# 保存图像
		from datetime import datetime
		op_time = datetime.now().strftime("%Y%m%d_%H%M%S")
		fig_path = os.path.join(output_dir, f"cm_{seed if seed is not None else self.random_state}_{op_time}.png")
		fig.savefig(fig_path, dpi=300, bbox_inches="tight")
		print(f"混淆矩阵图已保存到: {fig_path}")
		plt.close(fig)

		# 返回指标字典
		return {
			'accuracy': accuracy,
			'auc': auc,
			'confusion_matrix': cm,
			'classification_report': report
		}
	
	def plot_feature_importance(self, seed, top_n=20, output_dir='./output'):
		"""
		绘制特征重要度条形图
		:param top_n: 显示最重要的前N个特征，None表示全部
		:param output_dir: 输出目录
		"""
		if not self.is_trained:
			raise RuntimeError("模型尚未训练，请先调用 train() 方法")

		importances = self.model.feature_importances_
		indices = np.argsort(importances)[::-1]

		if top_n is not None:
			indices = indices[:top_n]
			top_features = [self.feature_names[i] for i in indices]
			top_importances = importances[indices]
		else:
			top_features = self.feature_names
			top_importances = importances

		plt.figure(figsize=(10, 6))
		plt.barh(range(len(top_importances)), top_importances[::-1], align='center')
		plt.yticks(range(len(top_importances)), top_features[::-1])
		plt.xlabel('Feature Importance')
		plt.title(f'Top {len(top_importances)} Feature Importances')
		plt.tight_layout()

		os.makedirs(output_dir, exist_ok=True)
		op_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
		fig_path = os.path.join(output_dir, f"feature_importance_{seed}_{op_time}.png")
		plt.savefig(fig_path, dpi=300, bbox_inches='tight')
		print(f"特征重要度图已保存到: {fig_path}")
		plt.close()

		# 打印特征重要度排序表
		print("\n特征重要度排序 (前10):")
		for i in range(min(10, len(top_importances))):
			print(f"{top_features[i]:30s}: {top_importances[i]:.6f}")

		return dict(zip(top_features, top_importances))

def main():
	# 预先配置
	random_state=42
	n_estimators=100
	max_depth=10
	prob_threshold=0.5
	top_n=20	# 最topN关键参数

	input_data_dir = r'E:\UDP2026\data\processed\input_nt_model\video'
	# 提取先前实验使用的种子
	exp_summary_log = r'E:\UDP2026\src\model\LSTM\output_info\video\summary_logs.csv'
	been_used_seed_list = []
	with open(exp_summary_log, 'r', encoding='utf-8') as f:
		reader = csv.DictReader(f)          # 按列名读取
		for row in reader:
			been_used_seed_list.append(row['seed'])
	exp_tests_list = read_tests_column(exp_summary_log)

	# 为每个目标业务创建输出环境
	scene = Path(input_data_dir).name
	scene_output_dir = os.path.join(OUTPUT_DIR, scene)
	os.makedirs(scene_output_dir, exist_ok=True)

	input_csv = [file for file in os.listdir(input_data_dir) if file.endswith('.csv')]
	csv_files = []
	for csv_file in input_csv:
		csv_files.append(os.path.join(input_data_dir, csv_file))

	# 对同样的种子进行多轮独立实验
	for seed, used_tests in zip(been_used_seed_list, exp_tests_list):
		used_seed = seed
		
		for i in range(len(used_tests)):
			used_tests[i] += '_combinedUDPModem_ws.csv'
			
		log_path = os.path.join(scene_output_dir, f"log_{used_seed}_{OP_TIME}.txt")
		with open(log_path, "w", encoding="utf-8") as f:
			tee = Tee(sys.stdout, f)
			with redirect_stdout(tee):
				print(f"日志保存到: {log_path}")

				print("使用设备：CPU")
				print(f'使用种子：{used_seed}')
				print(
					f"使用超参 | "
					f"random_state={random_state}, n_estimators={n_estimators}, max_depth={max_depth}, "
					f"prob_threshold={prob_threshold}, top_n={top_n}, "
				)
				# 1. 创建模型实例
				rf_model = RandomForestModel(random_state, n_estimators, max_depth)

				# 2. 加载数据
				print(f"使用测试集：{used_tests}")
				rf_model.load_data(csv_files, used_tests)

				# 3. 训练（自动划分训练/测试集）
				rf_model.train()

				# 4. 评估模型（输出混淆矩阵等）
				rf_model.evaluate(seed, prob_threshold, output_dir=scene_output_dir)

				# 5. 绘制特征重要度
				rf_model.plot_feature_importance(used_seed, top_n, output_dir=scene_output_dir)
			
			print(f"本次运行日志已保存到: {log_path}")

if __name__ == "__main__":
	main()