import csv
import ipaddress
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
import matplotlib.pyplot as plt

# 解决中文为方框与负号显示问题（若系统无字体可删掉）
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei"]
plt.rcParams["axes.unicode_minus"] = False
from pathlib import Path

def _ip_sort_key(ip: str):
	try:
		obj = ipaddress.ip_address(ip)
		return (obj.version, obj.packed)
	except Exception:
		return (99, ip.encode("utf-8", errors="ignore"))

@dataclass(frozen=True)
class FiveTuple:
	ip_a: str
	ip_b: str
	port_a: int
	port_b: int
	proto: str

	def label(self) -> str:
		return f"<{self.ip_a}:{self.port_a}, {self.ip_b}:{self.port_b}, {self.proto}>"

def canonical_5tuple(src_ip, dst_ip, src_port, dst_port, proto):
	left = (src_ip, int(src_port))
	right = (dst_ip, int(dst_port))
	if (_ip_sort_key(left[0]), left[1]) <= (_ip_sort_key(right[0]), right[1]):
		ip_a, port_a = left
		ip_b, port_b = right
	else:
		ip_a, port_a = right
		ip_b, port_b = left
	return FiveTuple(ip_a=ip_a, ip_b=ip_b, port_a=port_a, port_b=port_b, proto=str(proto).upper())

def flow_sta_vis(csv_path, top_n=20, min_count=1, output_png=None, show=True):
	"""
	读取extractor导出的csv，统计五元组流出现次数，绘制流结构分布图。
	:param csv_path: 输入csv文件路径
	:param top_n: 只显示出现次数最多的前N个流
	:param min_count: 最小出现次数
	:param output_png: 输出图片路径（默认同csv文件名）
	:param show: 是否弹窗显示
	"""
	csv_path = Path(csv_path)
	with csv_path.open("r", encoding="utf-8-sig") as f:
		reader = csv.DictReader(f)
		counts = Counter()
		for row in reader:
			proto = (row.get("protocol") or "").strip().upper()
			try:
				src_port = int(row.get("src_port") or 0)
				dst_port = int(row.get("dst_port") or 0)
			except Exception:
				continue
			src_ip = (row.get("src_ip") or "").strip()
			dst_ip = (row.get("dst_ip") or "").strip()
			if not src_ip or not dst_ip:
				continue
			ft = canonical_5tuple(src_ip, dst_ip, src_port, dst_port, proto)
			counts[ft] += 1
	# 过滤
	items = [(ft, cnt) for ft, cnt in counts.items() if cnt >= min_count]
	items = sorted(items, key=lambda x: x[1], reverse=True)
	if top_n:
		items = items[:top_n]
	if not items:
		print("[!] 没有可绘制的数据")
		return
	# 绘图
	plt.figure(figsize=(12, max(4, 0.4*len(items)+1)), dpi=120)
	y_pos = range(len(items))
	bar_colors = ["tab:blue" if ft.proto=="UDP" else "tab:orange" if ft.proto=="TCP" else "tab:gray" for ft, _ in items]
	plt.barh(y_pos, [cnt for _, cnt in items], color=bar_colors, alpha=0.85)
	plt.yticks(y_pos, [ft.label() for ft, _ in items], fontsize=9)
	plt.xlabel("出现次数")
	plt.title(f"五元组流结构统计 Top{top_n}")
	for y, (ft, cnt) in enumerate(items):
		plt.text(cnt, y, f" {cnt}", va="center", fontsize=9)
	plt.tight_layout()
	if not output_png:
		output_png = csv_path.with_name(f"{csv_path.stem}_flow_sta.png")
	plt.savefig(output_png, bbox_inches="tight")
	print(f"[✓] 已保存流结构统计图 -> {output_png}")
	if show:
		plt.show()

def export_top_flows(csv_path, top_n=20, min_count=1, output_csv=None):
	"""
	读取 extractor 导出的 csv，统计五元组流出现次数，并将 TopN 结果导出为 csv。

	:param csv_path: 输入 csv 文件路径
	:param top_n: 只导出出现次数最多的前 N 个流（None 或 0 表示导出全部满足条件的流）
	:param min_count: 最小出现次数
	:param output_csv: 输出 csv 文件路径（默认与输入同目录，文件名加 _topN_flows 后缀）
	:return: 实际输出的 csv 路径（Path 对象或 None）
	"""
	csv_path = Path(csv_path)
	with csv_path.open("r", encoding="utf-8-sig") as f:
		reader = csv.DictReader(f)
		counts = Counter()
		for row in reader:
			proto = (row.get("protocol") or "").strip().upper()
			try:
				src_port = int(row.get("src_port") or 0)
				dst_port = int(row.get("dst_port") or 0)
			except Exception:
				continue
			src_ip = (row.get("src_ip") or "").strip()
			dst_ip = (row.get("dst_ip") or "").strip()
			if not src_ip or not dst_ip:
				continue
			ft = canonical_5tuple(src_ip, dst_ip, src_port, dst_port, proto)
			counts[ft] += 1

	# 过滤 + 排序
	items = [(ft, cnt) for ft, cnt in counts.items() if cnt >= min_count]
	items = sorted(items, key=lambda x: x[1], reverse=True)
	if top_n:
		items = items[:top_n]
	if not items:
		print("[!] 没有可导出的数据")
		return None

	if not output_csv:
		label_n = top_n if top_n else "all"
		output_csv = csv_path.with_name(f"{csv_path.stem}_top{label_n}_flows.csv")

	fieldnames = ["ip_a", "ip_b", "port_a", "port_b", "proto", "count"]
	with Path(output_csv).open("w", newline="", encoding="utf-8-sig") as f_out:
		writer = csv.DictWriter(f_out, fieldnames=fieldnames)
		writer.writeheader()
		for ft, cnt in items:
			writer.writerow({
				"ip_a": ft.ip_a,
				"ip_b": ft.ip_b,
				"port_a": ft.port_a,
				"port_b": ft.port_b,
				"proto": ft.proto,
				"count": cnt,
			})

	print(f"[✓] 已导出 Top{top_n} 流统计 -> {output_csv}")
	return Path(output_csv)

def flow_struc_vis(csv_path, top_n=20, min_count=1, gap_ms=1000, output_png=None, show=True, title=None):
	"""
	读取extractor导出的csv，按五元组统计每个流的时间分布，绘制时间轴点+连线图。
	:param csv_path: 输入csv文件路径
	:param top_n: 只显示出现次数最多的前N个流
	:param min_count: 最小出现次数
	:param gap_ms: 连线阈值（毫秒，0为自动估计）
	:param output_png: 输出图片路径（默认同csv文件名）
	:param show: 是否弹窗显示
	:param title: 图标题
	"""
	from datetime import datetime
	csv_path = Path(csv_path)
	# 1. 统计五元组及其出现时间
	times_by_ft = {}
	counts = Counter()
	with csv_path.open("r", encoding="utf-8-sig") as f:
		reader = csv.DictReader(f)
		for row in reader:
			proto = (row.get("protocol") or "").strip().upper()
			try:
				src_port = int(row.get("src_port") or 0)
				dst_port = int(row.get("dst_port") or 0)
			except Exception:
				continue
			src_ip = (row.get("src_ip") or "").strip()
			dst_ip = (row.get("dst_ip") or "").strip()
			if not src_ip or not dst_ip:
				continue
			ft = canonical_5tuple(src_ip, dst_ip, src_port, dst_port, proto)
			try:
				ts = row.get("timestamp")
				dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
			except Exception:
				try:
					dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
				except Exception:
					continue
			times_by_ft.setdefault(ft, []).append(dt)
			counts[ft] += 1
	# 2. 过滤
	# 统计每个五元组的出现次数和时间跨度
	items = [(ft, cnt, sorted(times_by_ft[ft])) for ft, cnt in counts.items() if cnt >= min_count]
	def _span(item):
		times = item[2]
		if not times:
			return 0
		return (times[-1] - times[0]).total_seconds()
	# 先按n降序，再按时间跨度降序，n大且持续时间长的排在底部
	items = sorted(items, key=lambda x: (x[1], _span(x)))
	if top_n:
		items = items[-top_n:]
	items = items[::-1]  # 反转，n大且跨度长的在底部
	if not items:
		print("[!] 没有可绘制的数据")
		return
	# 3. 自动估计gap
	def _quantile(sorted_vals, q):
		if not sorted_vals:
			return 1.0
		if q <= 0:
			return sorted_vals[0]
		if q >= 1:
			return sorted_vals[-1]
		n = len(sorted_vals)
		pos = (n - 1) * q
		lo = int(pos)
		hi = min(lo + 1, n - 1)
		frac = pos - lo
		return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac
	if not gap_ms or gap_ms <= 0:
		# 估计gap
		deltas = []
		for _, _, times in items:
			if len(times) < 2:
				continue
			prev = times[0]
			for t in times[1:]:
				d = (t - prev).total_seconds()
				prev = t
				if d > 0 and d <= 10:
					deltas.append(d)
		deltas.sort()
		gap_s = max(0.2, min(3.0, _quantile(deltas, 0.9))) if deltas else 1.0
	else:
		gap_s = gap_ms / 1000.0
	# 4. 绘图
	plt.figure(figsize=(14, max(4, 0.35*len(items)+1.5)), dpi=130)
	ax = plt.gca()
	proto_colors = {"UDP": "tab:blue", "TCP": "tab:orange", "ICMP": "tab:green", "ICMPV6": "tab:green"}
	y_positions = list(range(len(items)))
	legend_protos = {}
	# 统一标签横坐标为全局时间中点
	all_times = [dt for _, _, times in items for dt in times]
	if all_times:
		all_times.sort()
		t_min, t_max = all_times[0], all_times[-1]
		x_center = t_min + (t_max - t_min) / 2 if t_min != t_max else t_min
	else:
		x_center = None
	for y, (ft, cnt, times) in zip(y_positions, items):
		color = proto_colors.get(ft.proto, "tab:gray")
		legend_protos.setdefault(ft.proto, color)
		# 散点
		ax.scatter(times, [y]*len(times), s=8, color=color, alpha=0.85)
		# 连线
		seg_start = times[0]
		prev = times[0]
		seg_len = 1
		segments = 0
		for t in times[1:]:
			gap = (t - prev).total_seconds()
			if gap <= gap_s:
				prev = t
				seg_len += 1
				continue
			if seg_len >= 2:
				ax.hlines(y, seg_start, prev, color=color, linewidth=2.2)
				segments += 1
			seg_start = t
			prev = t
			seg_len = 1
		if seg_len >= 2:
			ax.hlines(y, seg_start, prev, color=color, linewidth=2.2)
			segments += 1
		# 文本统一居中
		if x_center is not None:
			label = f"{ft.label()}  n={cnt}  seg={segments}"
			ax.text(x_center, y+0.18, label, fontsize=8, va="bottom", ha="center")
	ax.set_yticks([])
	ax.set_xlabel("时间戳")
	gap_ms_val = int(round(gap_s*1000))
	ax.set_title(title or f"五元组通信时间轴（聚类间隔={gap_ms_val}ms，共 {len(items)} 条）")
	ax.grid(True, axis="x", alpha=0.25)
	# 图例
	if legend_protos:
		from matplotlib.lines import Line2D
		handles = [Line2D([0], [0], color=c, lw=3, label=p) for p, c in sorted(legend_protos.items())]
		ax.legend(handles=handles, loc="upper left", fontsize=9)
	plt.gcf().autofmt_xdate()
	if not output_png:
		output_png = csv_path.with_name(f"{csv_path.stem}_flow_struc.png")
	plt.savefig(output_png, bbox_inches="tight")
	print(f"[✓] 已保存流结构时间轴图 -> {output_png}")
	if show:
		plt.show()

if __name__ == "__main__":
	test_csv = r'D:\XFC_files\code\UDP2026\output.csv'
	# 导出 TopN 流统计到 csv
	export_top_flows(test_csv, top_n=30, min_count=5)
	# 可视化示例：
	# flow_struc_vis(test_csv, top_n=30, min_count=5, gap_ms=1000, output_png=None, show=True, title=None)
	# flow_sta_vis(test_csv, top_n=30, min_count=5, output_png=None, show=True)