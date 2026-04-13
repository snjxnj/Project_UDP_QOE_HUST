import os
import pandas as pd

def log_timestamp_discontinuity(csv_dir, scene, timestamp_col='timestamp', threshold=1.0, log_path=None):
	"""
	统计每个csv文件中时间戳不连续点，输出日志：样本名、发生间断的时间戳、间断时长。
	:param csv_dir: csv 文件目录
	:param scene: 文件名前缀
	:param timestamp_col: 时间戳列名
	:param threshold: 超过该秒数视为间断
	:param log_path: 日志输出路径，若为None则打印到控制台
	"""
	all_files = [os.path.join(csv_dir, f) for f in os.listdir(csv_dir) if f.startswith(scene) and f.endswith('.csv')]
	logs = []
	last_file = None
	for file in all_files:
		try:
			df = pd.read_csv(file)
			if timestamp_col not in df.columns:
				print(f"{file} 无时间戳列 {timestamp_col}")
				continue
			ts_dt = pd.to_datetime(df[timestamp_col], errors='coerce')
			ts_dt = ts_dt.dropna()
			if len(ts_dt) < 2:
				continue
			gaps = []
			for i in range(1, len(ts_dt)):
				delta = ts_dt.iloc[i] - ts_dt.iloc[i-1]
				gap_sec = delta.total_seconds()
				gaps.append(gap_sec)
			sample_logs = []
			for idx, gap in enumerate(gaps):
				if gap > threshold:
					# 输出间断区间左端（gap发生前的时间戳）
					sample_logs.append(f"{os.path.basename(file)}\t{ts_dt.iloc[idx]}\t{gap:.3f}")
			if sample_logs:
				if last_file is not None:
					logs.append("")  # 样本间空行区分
				logs.extend(sample_logs)
				last_file = file
		except Exception as e:
			print(f"读取文件 {file} 失败: {e}")
	header = "样本名\t发生间断的时间戳\t间断时长"
	if log_path:
		with open(log_path, 'w', encoding='utf-8') as f:
			f.write(header + '\n')
			for line in logs:
				f.write(line + '\n')
		print(f"时间连续性检查日志已写入 {log_path}")
	else:
		print(header)
		for line in logs:
			print(line)

def print_features_variance(csv_dir, scene, timestamp_col='timestamp'):
	"""
	读取目录下所有以 gaming 开头的 csv 文件，拼接为一个大数据帧，
	计算除时间戳外所有特征的方差，并打印。
	:param csv_dir: csv 文件所在目录
	:param timestamp_col: 时间戳列名，默认 'timestamp'
	"""
	all_files = [os.path.join(csv_dir, f) for f in os.listdir(csv_dir) if f.startswith(scene) and f.endswith('.csv')]
	if not all_files:
		print(f'未找到 {scene} 开头的 csv 文件')
		return
	df_list = []
	for file in all_files:
		try:
			df = pd.read_csv(file)
			df_list.append(df)
		except Exception as e:
			print(f"读取文件 {file} 失败: {e}")
	if not df_list:
		print(f'所有 {scene} csv 文件均读取失败')
		return
	big_df = pd.concat(df_list, ignore_index=True)
	# 排除时间戳列
	feature_cols = [col for col in big_df.columns if col != timestamp_col]
	variances = big_df[feature_cols].var()
	variances_sorted = variances.sort_values()
	print(f'各特征({len(variances_sorted)}个)方差(升序):')
	pd.set_option('display.float_format', lambda x: '%.8f' % x)
	print(variances_sorted)
	pd.reset_option('display.float_format')

if __name__ == "__main__":
	test_dir = r'D:\XFC_files\code\UDP2026\data\processed\modem_info\caculated_modem_data'
	print_features_variance(test_dir, scene='gaming')
	log_timestamp_discontinuity(test_dir, scene='gaming', 
							 threshold=3.0, 
							 log_path=r'D:\XFC_files\code\UDP2026\data\processed\modem_info\time_discontinuity_log.txt')