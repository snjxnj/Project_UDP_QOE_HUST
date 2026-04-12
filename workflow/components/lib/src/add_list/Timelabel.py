import pandas as pd
import re

def to_seconds_hms(s: str) -> float:
	"""支持'YYYY-MM-DD HH:MM:SS(.ms)'或'HH:MM:SS(.ms)'或'HH-MM-SS(.ms)'字符串转为秒"""
	s = s.strip()
	# 如果包含日期，先只取时间部分
	if " " in s:
		s = s.split(" ")[-1]
	s = s.replace("-", ":")
	m = re.match(r"^(\d{1,2}):(\d{1,2}):(\d{1,2})(?:[\.:](\d{1,3}))?$", s)
	if not m:
		raise ValueError(f"时间格式不正确: {s}")
	h, mi, se, ms = m.groups()
	total = int(h) * 3600 + int(mi) * 60 + int(se)
	if ms is not None:
		total += int(ms) / (1000 if len(ms) == 3 else 10 ** len(ms))
	return float(total)

# 可能的时间列名集合
TIME_COLUMN_CANDIDATES = [
	'time', 'timestamp', 'frame_time', 'Time', 'Timestamp', 'time_str', 'timeStr'
]

def cut_df_by_time(df: pd.DataFrame, start_time: str, end_time: str, time_columns=None) -> pd.DataFrame:
	"""
	按时间字符串区间截取DataFrame
	:param df: 输入数据帧
	:param start_time: 开始时间字符串，如'20-02-11'或'20:02:11'
	:param end_time: 截止时间字符串，如'20-05-00'或'20:05:00'
	:param time_columns: 可选，时间列名list，优先使用，否则用默认集合
	:return: 截取后的DataFrame
	"""
	candidates = time_columns if time_columns is not None else TIME_COLUMN_CANDIDATES
	time_col = None
	for c in candidates:
		if c in df.columns:
			time_col = c
			break
	if time_col is None:
		raise ValueError(f"未找到时间列，候选列: {candidates}")
	times = df[time_col].astype(str).apply(to_seconds_hms)
	st = to_seconds_hms(start_time)
	et = to_seconds_hms(end_time)
	if st <= et:
		mask = (times >= st) & (times <= et)
	else:
		mask = (times >= st) | (times <= et)
	return df[mask].copy()

def read_label_intervals(label_path: str):
	"""读取标签文件，返回区间列表[(start_sec, end_sec), ...]"""
	intervals = []
	with open(label_path, encoding="utf-8") as f:
		for line in f:
			line = line.strip()
			if not line or line.startswith("#"):
				continue
			m = re.match(r"^(\d{2}:\d{2}:\d{2}(?:[\.:]\d{1,3})?)\s*-\s*(\d{2}:\d{2}:\d{2}(?:[\.:]\d{1,3})?)$", line)
			if not m:
				continue
			s1 = to_seconds_hms(m.group(1))
			s2 = to_seconds_hms(m.group(2))
			intervals.append((s1, s2))
	return intervals

def label_dataframe_by_intervals(df: pd.DataFrame, label_path: str, time_col_candidates=None) -> pd.DataFrame:
	"""
	给DataFrame按区间文件打标签，label=1为卡顿区间，label=0为正常
	:param df: 输入数据帧
	:param label_path: 标签区间文件路径
	:param time_col_candidates: 可选，时间列名list
	:return: 打标签后的数据帧
	"""
	if time_col_candidates is None:
		time_col_candidates = TIME_COLUMN_CANDIDATES
	time_col = None
	for c in time_col_candidates:
		if c in df.columns:
			time_col = c
			break
	if time_col is None:
		raise ValueError(f"未找到时间列，候选列: {time_col_candidates}")
	times = df[time_col].astype(str).apply(to_seconds_hms)
	intervals = read_label_intervals(label_path)
	labels = []
	for t in times:
		mark = 0
		for a, b in intervals:
			if a <= b:
				if a <= t <= b:
					mark = 1
					break
			else:
				if t >= a or t <= b:
					mark = 1
					break
		labels.append(mark)
	df = df.copy()
	df['label'] = labels
	# 添加label在最后一列
	cols = [col for col in df.columns if col != 'label'] + ['label']
	return df[cols]
