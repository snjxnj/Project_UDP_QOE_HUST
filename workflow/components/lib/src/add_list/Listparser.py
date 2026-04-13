import re
from src.add_list.Timelabel import to_seconds_hms

class SampleListParser:
	def __init__(self, filepath):
		self.filepath = filepath
		self.samples = self._parse_file()   # [{ID:...,src_Add:...,...},{},...]

	def _parse_file(self):
		"""
		解析add_list文件，构造样本list
		"""
		samples = []
		with open(self.filepath, encoding='utf-8') as f:
			for line in f:
				line = line.strip()
				# add_list中使用#注释
				if not line or line.startswith('#'):
					continue
				# 只处理包含ID:的规范样本行
				if line.startswith('ID:'):
					sample = self._parse_line(line)
					if sample:
						samples.append(sample)
		return samples
	
	def _parse_line(self, line):
		"""
		解析add_list文件每行，按逗号分割，提取key:value对
		"""
		features = {}
		for item in re.split(r',\s*', line):
			if ':' in item:
				key, value = item.split(':', 1)
				features[key.strip()] = value.strip().strip('"')
		return features if features else None
	
	def get_sample_info(self, id: str):
		"""
		根据指定样本ID获取样本特征字典 字典key：
	 	ID src_Add scene local_ip serv_ip start_time end_time lag_timeList_path
		"""
		for sample in self.samples:
			scene = sample.get('scene', '').strip()
			sample_id = sample.get('ID', '').strip()
			if scene and sample_id and f"{scene}_{sample_id[:10]}" == id:
				return sample
		return None

	def get_sample_duration(self, id:str):
		"""
		获取每个样本的有效实验时间长度
		"""
		sample_info = self.get_sample_info(id)
		st_s = sample_info['start_time']
		et_s = sample_info['end_time']

		st = to_seconds_hms(st_s)
		et = to_seconds_hms(et_s)

		# 跨零点：end 比 start 小，说明到了第二天
		if et >= st:
			duration = et - st
		else:
			duration = (24*3600 - st) + et  # 23:56:45 → 第二天 00:29:40

		return duration  # 单位：秒
	
	def use_sample_id(self, sample: dict) -> str:
		"""
		根据单条样本记录，返回形如 'video_2025122601' 的 scene_id10 字符串。
		"""
		scene = sample.get('scene', '').strip()
		sample_id = sample.get('ID', '').strip()
		if not scene or not sample_id:
			return ''
		return f"{scene}_{sample_id[:10]}"

	# 返回所有样本的特征字典列表
	def get_samples(self):
		return self.samples

	def __len__(self):
		return len(self.samples)

	def __getitem__(self, idx):
		return self.samples[idx]
