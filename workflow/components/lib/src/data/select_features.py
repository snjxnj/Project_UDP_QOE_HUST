import os
import glob
from typing import List

import pandas as pd

# 手动维护：需要保留的列名列表
# TODO: 按需填充/修改这个列表
KEEP_COLUMNS: List[str] = [
	'curTime_of_UTC8',
	'label',
	'num_send_packets',
	'avg_send_packetLen',
	'avg_send_packetInterval',
	'send_dataStream',
	'num_recv_packets',
	'avg_recv_packetLen',
	'avg_recv_packetInterval',
	'recv_dataStream',
	'rsrp_1_mean',
	'snr_1_mean',
	'mn_cqi_mean',
	'mn_bandwidth_mean',
	'mn_mac_mean',
	'mn_u_r_mean',
	'mn_d_r_mean',
	'mn_u_m_mean',
	'mn_d_m_mean',
	'mn_d_g_mean',
    'mn_u_g_mean',
	'mn_u_b_mean',
	'mn_d_b_mean',
]
"""
候选：
curTime_of_UTC8,curWindow,label,

num_send_packets,avg_send_packetLen,send_dataStream,avg_send_packetInterval,max_send_packetInterval,min_send_packetInterval,
std_send_packetInterval,cv_send_packetInterval,send_Mutation_of_numPackets

num_recv_packets,avg_recv_packetLen,recv_dataStream,avg_recv_packetInterval,max_recv_packetInterval,min_recv_packetInterval,
std_recv_packetInterval,cv_recv_packetInterval,recv_Mutation_of_numPackets,

mn_cc,mn_redrt,mn_rre_count,mn_rach_fail,mn_rlf_num,mn_ota_irat_mean,
mn_irat_mean,mn_u_r_mean,mn_d_r_mean,mn_u_m_mean,mn_d_m_mean,mn_d_g_mean,
mn_u_g_mean,mn_u_b_mean,mn_d_b_mean,mn_txp_md_mean,mn_mtpl_mean,mn_cqi_mean,
mn_mac_mean,mn_pu_m_l_mean,mn_pd_m_l_mean,mn_rlf_cause_mean,mn_retr_mean,
mn_pp_s_mean,mn_pp_dis_mean,mn_pp_r_mean,mn_ri_mean,mn_prach_cfg_mean,
mn_pre_for_mean,mn_o_fail_mean,mn_ph_dis_mean,mn_ssnr_mean,mn_tsnr_mean,
mn_path_loss_mean,mn_target_pwr_mean,mn_bandwidth_mean,rf_sul_state,rf_sul_band,
mn_ver,rsrp_1_mean,rsrp_2_mean,rsrp_3_mean,rsrp_4_mean,snr_1_mean,snr_2_mean,
snr_3_mean,snr_4_mean
"""
# 默认输入/输出目录
DEFAULT_INPUT_DIR = r"D:\XFC_files\code\UDP2026\data\processed\combine_info"
DEFAULT_OUTPUT_DIR = r"D:\XFC_files\code\UDP2026\data\processed\input_model"

def select_columns_for_file(in_path: str, out_root: str, keep_columns: List[str]) -> str | None:
	"""
	读取单个 CSV，只保留 keep_columns 中指定的列，
	并根据文件名下划线分割的第 1 段（业务名）创建子目录后输出。
	"""
	try:
		df = pd.read_csv(in_path)
	except Exception as e:
		print(f"读取失败: {in_path} | {e}")
		return None

	if not keep_columns:
		print(f"警告：KEEP_COLUMNS 为空，跳过文件 {in_path}")
		return None

	cols_to_keep = [c for c in df.columns if c in keep_columns]
	if not cols_to_keep:
		print(f"警告：{os.path.basename(in_path)} 中无需要保留的列，跳过")
		return None

	selected = df[cols_to_keep]

	base = os.path.basename(in_path)
	parts = base.split("_")
	business = parts[0] if parts else "unknown"

	subdir = os.path.join(out_root, business)
	os.makedirs(subdir, exist_ok=True)

	out_path = os.path.join(subdir, base)
	selected.to_csv(out_path, index=False, encoding="utf-8")
	print(f"已输出: {out_path} (保留列数 {len(cols_to_keep)})")
	return out_path

def process_all_csv(
	input_dir: str = DEFAULT_INPUT_DIR,
	output_dir: str = DEFAULT_OUTPUT_DIR,
	keep_columns: List[str] = KEEP_COLUMNS,
) -> None:
	"""遍历 input_dir 下所有 CSV 文件，按列筛选后输出到 output_dir/业务子目录。"""
	os.makedirs(output_dir, exist_ok=True)

	pattern = os.path.join(input_dir, "*.csv")
	files = glob.glob(pattern)
	if not files:
		print(f"未在 {input_dir} 找到任何 CSV 文件")
		return

	print(f"在 {input_dir} 下找到 {len(files)} 个 CSV 文件，开始筛选列并导出为后续投入模型训练分析的数据文件...")
	for f in sorted(files):
		try:
			select_columns_for_file(f, output_dir, keep_columns)
		except Exception as e:
			print(f"处理失败 {f}: {e}")

if __name__ == "__main__":
	process_all_csv()