#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
CSV 异常值扫描工具
- 递归扫描指定目录下的 .csv 文件
- 检测每个单元格是否包含 NaN / Inf / -Inf / Infinity（大小写不敏感）
- 可选：检测数值占位（如 -99 / -111 / 99 等）
- 支持控制台打印与写入报告 CSV

用法示例（Windows CMD）：
  python My_lib\tools\check_csv_anomalies.py --dir d:\XFC_files\code\UDP_QoE\Modem_data_PreProcessing\caculated_modem_data --recursive --report d:\XFC_files\code\UDP_QoE\TEMP\csv_anomaly_report.csv
  
仅检测 NaN/Inf：
  python My_lib\tools\check_csv_anomalies.py --dir d:\path\to\csvs

启用占位值（-99,-111,99）：
  python My_lib\tools\check_csv_anomalies.py --dir d:\path\to\csvs --check-sentinels --sentinels -99 -111 99
"""
from __future__ import annotations
import argparse
import csv
import os
import sys
from pathlib import Path
from typing import List, Tuple, Dict, Any

DEFAULT_PATTERNS = [
    "nan",         # NaN
    "inf",         # Inf / -Inf / Infinity（使用包含匹配）
]

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="扫描目录中的 CSV，报告包含 NaN/Inf 的单元格")
    p.add_argument("--dir", required=True, help="包含 CSV 的目录路径")
    p.add_argument("--recursive", action="store_true", help="递归扫描子目录")
    p.add_argument("--report", help="将扫描结果写入此 CSV 报告文件（可选）")
    p.add_argument("--encoding", default="utf-8", help="CSV 文件编码，默认 utf-8")
    p.add_argument("--delimiter", default=",", help="CSV 分隔符，默认 ,")
    p.add_argument("--check-sentinels", action="store_true", help="是否额外检测占位值（如 -99/-111/99）")
    p.add_argument("--sentinels", nargs="*", type=float, default=[-99.0, -111.0, 99.0], help="自定义占位值列表")
    p.add_argument("--quiet-ok", action="store_true", help="无异常文件不打印 OK 行")
    return p.parse_args()

def find_csv_files(root: Path, recursive: bool) -> List[Path]:
    if recursive:
        return [p for p in root.rglob("*.csv") if p.is_file()]
    else:
        return [p for p in root.glob("*.csv") if p.is_file()]

def cell_contains_patterns(cell: str, patterns: List[str]) -> bool:
    s = (cell or "").strip().lower()
    if not s:
        return False
    # 去掉常见包裹符，降低误报概率
    s = s.replace("{", " ").replace("}", " ")
    # 直接包含匹配（例如 "-inf"、"infinity" 都会命中 "inf"）
    return any(p in s for p in patterns)

def extract_numbers_from_cell(cell: str) -> List[float]:
    """尽量从单元格中解析出所有数值，兼容 {a,b} 或 "a,b,c" 形式。解析失败忽略。"""
    s = (cell or "").strip()
    if not s:
        return []
    # 去掉花括号，按逗号切分
    s = s.replace("{", "").replace("}", "")
    parts = [t.strip() for t in s.split(",")]
    vals: List[float] = []
    for t in parts:
        if not t:
            continue
        tl = t.lower()
        # 跳过明显的 NaN/Inf 字样
        if tl in ("nan", "+nan", "-nan"):
            continue
        if "inf" in tl:
            continue
        try:
            vals.append(float(t))
        except ValueError:
            # 单元格可能是纯字符串（例如 rf_sul_state），忽略即可
            continue
    return vals

def scan_csv(file_path: Path, encoding: str, delimiter: str, patterns: List[str], check_sentinels: bool, sentinels: List[float]) -> Tuple[List[Dict[str, Any]], int]:
    anomalies: List[Dict[str, Any]] = []
    n_rows = 0
    try:
        with file_path.open("r", encoding=encoding, newline="") as f:
            reader = csv.reader(f, delimiter=delimiter)
            try:
                header = next(reader)
            except StopIteration:
                return anomalies, 0
            col_count = len(header)
            # 从第2行起计数（包含表头的文件，数据行号=2开始）
            row_idx = 1
            for row in reader:
                row_idx += 1
                n_rows += 1
                # 对齐列数
                if len(row) < col_count:
                    row = row + [""] * (col_count - len(row))
                elif len(row) > col_count:
                    row = row[:col_count]
                for j, cell in enumerate(row):
                    col_name = header[j] if j < len(header) else f"col_{j}"
                    # 1) 字符异常：是否包含 NaN/Inf 等字样
                    if cell_contains_patterns(cell, patterns):
                        anomalies.append({
                            "file": str(file_path),
                            "row": row_idx,
                            "column": col_name,
                            "type": "STRING_PATTERN",
                            "value": cell
                        })
                        continue
                    # 2) 可选：占位值检测
                    if check_sentinels:
                        nums = extract_numbers_from_cell(cell)
                        if nums:
                            for v in nums:
                                if any(abs(v - s) < 1e-9 for s in sentinels):
                                    anomalies.append({
                                        "file": str(file_path),
                                        "row": row_idx,
                                        "column": col_name,
                                        "type": "SENTINEL",
                                        "value": v
                                    })
                                    # 不 break，单元格可能有多个占位值
    except UnicodeDecodeError as e:
        anomalies.append({
            "file": str(file_path),
            "row": 1,
            "column": "*",
            "type": "ENCODING_ERROR",
            "value": str(e)
        })
    except Exception as e:
        anomalies.append({
            "file": str(file_path),
            "row": 1,
            "column": "*",
            "type": "READ_ERROR",
            "value": str(e)
        })
    return anomalies, n_rows

def write_report(report_path: Path, records: List[Dict[str, Any]]):
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file", "row", "column", "type", "value"])
        for r in records:
            w.writerow([r.get("file", ""), r.get("row", ""), r.get("column", ""), r.get("type", ""), r.get("value", "")])

def main():
    args = parse_args()
    root = Path(args.dir)
    if not root.exists() or not root.is_dir():
        print(f"[ERROR] 目录不存在: {root}")
        sys.exit(2)

    csv_files = find_csv_files(root, args.recursive)
    if not csv_files:
        print(f"[INFO] 未在 {root} 找到 .csv 文件")
        return

    patterns = [p.lower() for p in DEFAULT_PATTERNS]
    all_records: List[Dict[str, Any]] = []
    total_rows = 0
    total_files = 0
    files_with_anomaly = 0

    for fp in csv_files:
        total_files += 1
        recs, n_rows = scan_csv(
            fp,
            encoding=args.encoding,
            delimiter=args.delimiter,
            patterns=patterns,
            check_sentinels=args.check_sentinels,
            sentinels=args.sentinels,
        )
        total_rows += n_rows
        all_records.extend(recs)
        if recs:
            files_with_anomaly += 1
            print(f"[ALERT] {fp} -> {len(recs)} 条异常")
        else:
            if not args.quiet_ok:
                print(f"[OK]    {fp}")

    print("\n==== 扫描汇总 ====")
    print(f"文件数: {total_files}")
    print(f"总行数(不含表头): {total_rows}")
    print(f"异常文件: {files_with_anomaly}")
    print(f"异常记录: {len(all_records)}")

    if args.report:
        report_path = Path(args.report)
        write_report(report_path, all_records)
        print(f"报告已写入: {report_path}")

if __name__ == "__main__":
    main()
