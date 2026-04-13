from scapy.all import PcapReader, IP, IPv6, TCP, UDP
import os
import csv

def pcap_to_csv(root_dir, csv_path, max_packets=0):
	"""
	递归读取目录下所有 .pcap 文件，将每个包的关键信息写入csv。
	:param root_dir: 根目录路径
	:param csv_path: 输出csv文件路径
	:param max_packets: 每个文件最多写入多少个包，0为不限制
	"""
	from scapy.layers.inet import ICMP
	from scapy.layers.inet6 import ICMPv6EchoRequest, ICMPv6EchoReply
	from scapy.layers.dns import DNS
	import datetime
	rows = []
	for dirpath, _, filenames in os.walk(root_dir):
		for fname in filenames:
			ext = os.path.splitext(fname)[1].lower()
			if ext.startswith('.pcap'):
				fpath = os.path.join(dirpath, fname)
				try:
					count = 0
					with PcapReader(fpath) as pcap:
						for pkt in pcap:
							ts = pkt.time if hasattr(pkt, 'time') else None
							proto = None
							src_ip = dst_ip = src_port = dst_port = None
							extra_info = ''
							pkt_len = len(pkt) if hasattr(pkt, '__len__') else None
							if IP in pkt:
								src_ip = pkt[IP].src
								dst_ip = pkt[IP].dst
								if TCP in pkt:
									proto = 'TCP'
									src_port = pkt[TCP].sport
									dst_port = pkt[TCP].dport
								elif UDP in pkt:
									if pkt[UDP].sport == 53 or pkt[UDP].dport == 53 or DNS in pkt:
										proto = 'DNS'
										src_port = pkt[UDP].sport
										dst_port = pkt[UDP].dport
										if DNS in pkt and hasattr(pkt[DNS], 'qd') and pkt[DNS].qdcount > 0:
											try:
												qname = pkt[DNS].qd.qname.decode() if hasattr(pkt[DNS].qd.qname, 'decode') else str(pkt[DNS].qd.qname)
											except Exception:
												qname = str(pkt[DNS].qd.qname)
											extra_info += f"DNS查询: {qname}"
										if DNS in pkt and hasattr(pkt[DNS], 'an') and pkt[DNS].ancount > 0:
											answers = []
											for i in range(pkt[DNS].ancount):
												rr = pkt[DNS].an[i]
												if hasattr(rr, 'rdata') and hasattr(rr, 'rrname'):
													try:
														rrname = rr.rrname.decode() if hasattr(rr.rrname, 'decode') else str(rr.rrname)
													except Exception:
														rrname = str(rr.rrname)
													answers.append(f"{rrname}->{rr.rdata}")
											if answers:
												extra_info += f"; DNS解析: {'; '.join(answers)}"
									else:
										proto = 'UDP'
										src_port = pkt[UDP].sport
										dst_port = pkt[UDP].dport
								elif ICMP in pkt:
									proto = 'ICMP'
									icmp_type = pkt[ICMP].type
									icmp_code = pkt[ICMP].code
									icmp_desc = ''
									if icmp_type == 8:
										icmp_desc = 'Echo Request'
									elif icmp_type == 0:
										icmp_desc = 'Echo Reply'
									elif icmp_type == 3:
										icmp_desc = 'Destination Unreachable'
									extra_info += f"ICMP type: {icmp_type}({icmp_desc}), code: {icmp_code}"
								else:
									proto = pkt[IP].proto
							elif IPv6 in pkt:
								src_ip = pkt[IPv6].src
								dst_ip = pkt[IPv6].dst
								if TCP in pkt:
									proto = 'TCP'
									src_port = pkt[TCP].sport
									dst_port = pkt[TCP].dport
								elif UDP in pkt:
									if pkt[UDP].sport == 53 or pkt[UDP].dport == 53 or DNS in pkt:
										proto = 'DNS'
										src_port = pkt[UDP].sport
										dst_port = pkt[UDP].dport
										if DNS in pkt and hasattr(pkt[DNS], 'qd') and pkt[DNS].qdcount > 0:
											try:
												qname = pkt[DNS].qd.qname.decode() if hasattr(pkt[DNS].qd.qname, 'decode') else str(pkt[DNS].qd.qname)
											except Exception:
												qname = str(pkt[DNS].qd.qname)
											extra_info += f"DNS查询: {qname}"
										if DNS in pkt and hasattr(pkt[DNS], 'an') and pkt[DNS].ancount > 0:
											answers = []
											for i in range(pkt[DNS].ancount):
												rr = pkt[DNS].an[i]
												if hasattr(rr, 'rdata') and hasattr(rr, 'rrname'):
													try:
														rrname = rr.rrname.decode() if hasattr(rr.rrname, 'decode') else str(rr.rrname)
													except Exception:
														rrname = str(rr.rrname)
													answers.append(f"{rrname}->{rr.rdata}")
											if answers:
												extra_info += f"; DNS解析: {'; '.join(answers)}"
									else:
										proto = 'UDP'
										src_port = pkt[UDP].sport
										dst_port = pkt[UDP].dport
								elif ICMPv6EchoRequest in pkt or ICMPv6EchoReply in pkt:
									proto = 'ICMPv6'
									icmpv6_type = pkt[ICMPv6EchoRequest].type if ICMPv6EchoRequest in pkt else pkt[ICMPv6EchoReply].type if ICMPv6EchoReply in pkt else None
									icmpv6_code = pkt[ICMPv6EchoRequest].code if ICMPv6EchoRequest in pkt else pkt[ICMPv6EchoReply].code if ICMPv6EchoReply in pkt else None
									icmpv6_desc = ''
									if icmpv6_type == 128:
										icmpv6_desc = 'Echo Request'
									elif icmpv6_type == 129:
										icmpv6_desc = 'Echo Reply'
									elif icmpv6_type == 1:
										icmpv6_desc = 'Destination Unreachable'
									if icmpv6_type is not None:
										extra_info += f"ICMPv6 type: {icmpv6_type}({icmpv6_desc}), code: {icmpv6_code}"
								else:
									proto = pkt[IPv6].nh
									if proto == 58:
										proto = 'ICMPv6'
										if hasattr(pkt, 'type') and hasattr(pkt, 'code'):
											icmpv6_type = pkt.type
											icmpv6_code = pkt.code
											icmpv6_desc = ''
											if icmpv6_type == 128:
												icmpv6_desc = 'Echo Request'
											elif icmpv6_type == 129:
												icmpv6_desc = 'Echo Reply'
											elif icmpv6_type == 1:
												icmpv6_desc = 'Destination Unreachable'
											extra_info += f"ICMPv6 type: {icmpv6_type}({icmpv6_desc}), code: {icmpv6_code}"
							else:
								continue
							if ts is not None:
								ts_str = datetime.datetime.fromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
							else:
								ts_str = 'None'
							pkt_len_str = pkt_len if pkt_len is not None else ''
							rows.append([
								fname, ts_str, src_ip, src_port, dst_ip, dst_port, proto, pkt_len_str, extra_info
							])
							count += 1
							if max_packets > 0 and count >= max_packets:
								break
				except Exception as e:
					print(f"读取文件 {fpath} 失败: {e}")
	# 写入csv
	with open(csv_path, 'w', newline='', encoding='utf-8') as f:
		writer = csv.writer(f)
		writer.writerow(['src_file', 'timestamp', 'src_ip', 'src_port', 'dst_ip', 'dst_port', 'protocol', 'packet_length', 'extra_info'])
		writer.writerows(rows)

if __name__ == "__main__":
	test_dir = r'D:\XFC_files\code\UDP_QoE\original_data_set\set1\test_gaming_2025101701'
	# 示例：打印五元组
	# print_pcap_five_tuple(test_dir, max_packets=10)
	# 示例：导出为csv
	pcap_to_csv(test_dir, 'output.csv', max_packets=0)