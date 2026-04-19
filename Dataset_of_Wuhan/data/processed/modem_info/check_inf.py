import pathlib
import csv


def main() -> None:
	# 需要检查的目录
	root = pathlib.Path(r"D:\XFC_files\code\UDP2026\data\processed\modem_info\csv_from_all_logs")

	if not root.exists():
		print(f"目录不存在: {root}")
		return

	total_files = 0
	matched_files = 0
	all_matched_columns: set[str] = set()

	for csv_path in root.rglob("*.csv"):
		total_files += 1
		try:
			with csv_path.open("r", encoding="utf-8", newline="") as f:
				reader = csv.reader(f)
				header = next(reader, None)
				rows = list(reader)
		except UnicodeDecodeError:
			# 尝试 gbk 作为备用编码
			try:
				with csv_path.open("r", encoding="gbk", newline="") as f:
					reader = csv.reader(f)
					header = next(reader, None)
					rows = list(reader)
			except Exception as e:  # noqa: BLE001
				print(f"读取失败（编码问题）：{csv_path} -> {e}")
				continue
		except Exception as e:  # noqa: BLE001
			print(f"读取失败：{csv_path} -> {e}")
			continue

		if not header:
			print(f"空表头，跳过：{csv_path}")
			continue

		# 检查每个单元格字符串内容中是否包含 "inf"（不区分大小写）
		file_matched_columns: set[str] = set()
		file_matched_cells = 0

		for row_idx, row in enumerate(rows, start=2):
			for col_name, cell in zip(header, row):
				if "inf" in str(cell).lower():
					file_matched_cells += 1
					file_matched_columns.add(col_name)
					# 为避免输出过多，仅展示每个文件前若干条示例
					if file_matched_cells <= 10:
						print("==============================")
						print(f"文件：{csv_path}")
						print(f"  行 {row_idx}, 列 {col_name} 含有 'inf'：{cell}")

		if file_matched_cells > 0:
			matched_files += 1
			all_matched_columns.update(file_matched_columns)
			print(f"该文件中共发现包含 'inf' 的单元格：{file_matched_cells}")

	print("==============================")
	print(f"总共扫描 CSV 文件数：{total_files}")
	print(f"含有 'inf' 单元格的文件数：{matched_files}")
	if all_matched_columns:
		print("所有出现过 'inf' 的列名集合：")
		for col in sorted(all_matched_columns):
			print(f"  - {col}")
	else:
		print("未在任何表头中找到包含 'inf' 的字段。")


if __name__ == "__main__":
	main()

