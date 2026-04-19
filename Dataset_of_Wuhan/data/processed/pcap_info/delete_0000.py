import pathlib


def main() -> None:
	# 脚本所在目录：data/processed/pcap_info
	root = pathlib.Path(__file__).resolve().parent
	count = 0

	# 递归查找当前目录及子目录下所有以 0000_ 开头的 .csv 文件
	for csv_path in root.rglob("0000_*.csv"):
		new_name = csv_path.name.replace("0000_", "", 1)
		new_path = csv_path.with_name(new_name)

		if new_path.exists():
			print(f"跳过：目标已存在 {new_path}")
			continue

		csv_path.rename(new_path)
		count += 1
		print(f"重命名: {csv_path} -> {new_path}")

	print(f"完成，累计重命名 {count} 个文件")


if __name__ == "__main__":
	main()

