import csv
import re
from pathlib import Path

def ip_parser(csv_path, txt_path=None):
	"""
	解析extractor导出的csv文件，提取DNS协议的ip-域名映射，导出为txt文件。
	:param csv_path: 输入csv文件路径
	:param txt_path: 输出txt文件路径（默认同名，后缀为_ip_map.txt）
	"""
	csv_path = Path(csv_path)
	if not txt_path:
		txt_path = csv_path.with_name(f"{csv_path.stem}_ip_map.txt")
	mapping = set()
	with csv_path.open("r", encoding="utf-8-sig") as f:
		reader = csv.DictReader(f)
		for row in reader:
			proto = (row.get("protocol") or "").strip().upper()
			if proto != "DNS":
				continue
			extra_info = row.get("extra_info") or ""
			# 匹配 DNS解析: 域名->IP; 域名->IP ...
			for m in re.finditer(r"([\w\.-]+)\s*->\s*([\w\.:']+)", extra_info):
				domain, ip = m.group(1), m.group(2)
				# 去除b'xxx.'字节串前缀
				if ip.startswith("b'") and ip.endswith("'."):
					ip = ip[2:-2]
				elif ip.startswith("b'") and ip.endswith("'"):
					ip = ip[2:-1]
				mapping.add(f"{ip}\t{domain}")
	# 写入txt
	with open(txt_path, "w", encoding="utf-8") as f:
		for line in sorted(mapping):
			f.write(line + "\n")
	print(f"[✓] 已导出IP-域名映射到: {txt_path}")

if __name__ == "__main__":
    test_csv = r'D:\XFC_files\code\UDP2026\output.csv'
    ip_parser(test_csv)