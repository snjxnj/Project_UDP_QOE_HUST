import ast
import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class LogSummaryRow:
	txt_file: str
	seed: Optional[int]
	tests: Optional[List[str]]
	rate_0: Optional[float]
	rate_1: Optional[float]


_RE_SEED_USED = re.compile(r"\bused_seed\s*=\s*(\d+)")
_RE_SEED_CN = re.compile(r"使用种子：\s*(\d+)")
_RE_TESTS = re.compile(r"使用测试集：\s*(\[.*\])\s*$")
_RE_RATE0 = re.compile(r"^非卡顿判别:.*=\s*([0-9]*\.?[0-9]+)")
_RE_RATE1 = re.compile(r"^卡顿判别:.*=\s*([0-9]*\.?[0-9]+)")


def _safe_float(s: str) -> Optional[float]:
	try:
		return float(s)
	except Exception:
		return None


def parse_log_txt(file_path: str) -> LogSummaryRow:
	"""Parse one training log (.txt) and return summary fields.

	Expected patterns (examples):
	  - used_seed = 184308126
	  - 使用种子：184308126
	  - 使用测试集：['a.csv', 'b.csv', 'c.csv']
	  - 非卡顿判别: ... = 0.9660
	  - 卡顿判别: ... = 0.9053
	"""
	seed: Optional[int] = None
	tests: Optional[List[str]] = None
	rate_0: Optional[float] = None
	rate_1: Optional[float] = None

	# Some logs may be huge; line-by-line parsing is safer.
	with open(file_path, "r", encoding="utf-8", errors="replace") as f:
		for line in f:
			if seed is None:
				m = _RE_SEED_USED.search(line)
				if m:
					seed = int(m.group(1))
				else:
					m = _RE_SEED_CN.search(line)
					if m:
						seed = int(m.group(1))

			if tests is None:
				m = _RE_TESTS.search(line.strip())
				if m:
					raw = m.group(1)
					try:
						value = ast.literal_eval(raw)
						if isinstance(value, list) and all(isinstance(x, str) for x in value):
							tests = value
					except Exception:
						tests = None

			stripped = line.strip()

			if rate_0 is None:
				m = _RE_RATE0.search(stripped)
				if m:
					rate_0 = _safe_float(m.group(1))

			if rate_1 is None:
				m = _RE_RATE1.search(stripped)
				if m:
					rate_1 = _safe_float(m.group(1))

	return LogSummaryRow(
		txt_file=os.path.basename(file_path),
		seed=seed,
		tests=tests,
		rate_0=rate_0,
		rate_1=rate_1,
	)


def summarize_directory(dir_path: str) -> List[LogSummaryRow]:
	rows: List[LogSummaryRow] = []
	for name in sorted(os.listdir(dir_path)):
		if not name.lower().endswith(".txt"):
			continue
		file_path = os.path.join(dir_path, name)
		if not os.path.isfile(file_path):
			continue
		rows.append(parse_log_txt(file_path))
	return rows


def write_summary_csv(rows: List[LogSummaryRow], output_csv_path: str) -> None:
	os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
	rows_sorted = sorted(
		rows,
		key=lambda r: (
			-1.0 if r.rate_1 is None else -float(r.rate_1),
			-1.0 if r.rate_0 is None else -float(r.rate_0),
			r.txt_file,
		),
	)
	with open(output_csv_path, "w", encoding="utf-8", newline="") as f:
		writer = csv.writer(f)
		writer.writerow(["txt_file", "seed", "tests", "rate_0", "rate_1"])
		for r in rows_sorted:
			res = []
			if r.tests is not None:
				for test in r.tests:
					base = test[:-4]   # 去掉 ".csv"
					# 按下划线分割
					parts = base.split('_')
					res.append(parts[0]+'_'+parts[1])
				
			tests_str = ";".join(res) if res else ""
			writer.writerow([
				r.txt_file,
				"" if r.seed is None else r.seed,
				tests_str,
				"" if r.rate_0 is None else f"{r.rate_0:.6f}",
				"" if r.rate_1 is None else f"{r.rate_1:.6f}",
			])


def main() -> None:
	base_dir = r'E:\UDP2026\src\model\LSTM\output_info\meeting'
	rows = summarize_directory(base_dir)
	# As requested: write one CSV file into the script directory.
	output_csv_path = os.path.join(base_dir, "summary_logs.csv")
	write_summary_csv(rows, output_csv_path)

	ok = sum(1 for r in rows if (r.seed is not None and r.tests and r.rate_0 is not None and r.rate_1 is not None))
	print(f"已扫描 .txt 文件数: {len(rows)}")
	print(f"成功解析完整字段数: {ok}")
	print(f"汇总 CSV 已保存: {output_csv_path}")


if __name__ == "__main__":
	main()