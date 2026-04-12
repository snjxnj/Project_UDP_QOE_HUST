
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def plot_features_from_csv(csv_path, output_root=r'D:\XFC_files\code\UDP2026\data\visualization', lag_file=None):
	"""
	调用该函数读取单个csv文件，对里面的多列特征绘制折线图，横轴为时间，每个特征单独成图。
	输出路径为 output_root/csv源文件名/特征名.png
	例如：传入 csv_path='.../gaming_2025102601_mean.csv'
	则输出路径为 'output_root/gaming_2025102601_mean/各数据特征的可视化图片'
	支持多种常见时间戳列名。
	"""
	print(f'正在绘制csv文件可视化: {csv_path}')
	# 读取csv
	df = pd.read_csv(csv_path)
	# 支持的时间戳列名
	time_col_candidates = ['curTime_of_UTC8', 'timestamp', 'time', 'datetime', 'ts']
	time_col = None
	for col in time_col_candidates:
		if col in df.columns:
			time_col = col
			break
	if time_col is None:
		raise ValueError(f'csv文件缺少时间戳列，支持列名: {time_col_candidates}')
	df[time_col] = pd.to_datetime(df[time_col])

	# 解析lag_file（卡顿区间）
	lag_intervals = []
	if lag_file is not None and os.path.exists(lag_file):
		import re
		import datetime
		# 尝试从lag_file文件名中提取日期
		lag_file_name = os.path.basename(lag_file)
		date_str = None
		# 例如 meeting_sbad_sgood_2025102501_lag_timeList.txt
		parts = lag_file_name.split('_')
		for part in parts:
			if part.isdigit() and len(part) >= 8:
				date_str = part[:8]
				break
		if date_str is None:
			# 默认用csv的第一行日期
			date_str = df[time_col].iloc[0].strftime('%Y%m%d')
		time_interval_pattern = r'^([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})-([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3})$'
		with open(lag_file, 'r', encoding='utf-8') as f:
			for line in f:
				line = line.strip().replace('"', '')
				m = re.match(time_interval_pattern, line)
				if m:
					start_time, end_time = m.groups()
					start_dt = datetime.datetime.strptime(f"{date_str} {start_time}", "%Y%m%d %H:%M:%S.%f")
					end_dt = datetime.datetime.strptime(f"{date_str} {end_time}", "%Y%m%d %H:%M:%S.%f")
					lag_intervals.append((start_dt, end_dt))

	# 过滤要绘制的特征列
	columns_to_plot = [col for col in df.columns if col not in time_col_candidates + ['curWindow']]
	# 输出文件夹
	csv_name = os.path.splitext(os.path.basename(csv_path))[0]
	output_dir = os.path.join(output_root, csv_name)
	os.makedirs(output_dir, exist_ok=True)
	# 绘图
	for column in columns_to_plot:
		plt.figure(figsize=(10,6))
		ax = plt.gca()
		# 绘制卡顿区间红色背景
		if lag_intervals:
			for start_dt, end_dt in lag_intervals:
				ax.axvspan(start_dt, end_dt, color='red', alpha=0.3)
		plt.plot(df[time_col], df[column], linestyle='-', color='blue', label=column)
		plt.title(f'{column}')
		plt.xlabel('time')
		plt.ylabel(f'{column}')
		plt.legend()
		plt.grid(True)
		plt.tight_layout()
		plt.savefig(os.path.join(output_dir, f'{column}.png'), dpi=300, bbox_inches='tight')
		plt.close()

if __name__ == "__main__":
	csvFile_path = r'D:\XFC_files\code\UDP2026\data\processed\modem_info\caculated_modem_data\gaming_2025102601_mean.csv'
	plot_features_from_csv(csvFile_path)
