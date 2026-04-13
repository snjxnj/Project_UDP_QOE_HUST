import re
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict
import src.pcap.tshark.ip_ranks as ip
import src.add_list.Listparser as parser
import config.Vertically_align_addlist as shape
"""
2026.3.11:
addlist总控表需要人工填写 
该脚本考虑尽可能自动化该过程
"""
FIELDS = [
    "ID",
    "src_Add",
    "scene",
    "local_ip",
    "serv_ip",
    "start_time",
    "end_time",
    "lag_timeList_path",
]

def find_label_txt_files(root_dir: str | Path) -> Dict[str, str]:
    """
    递归地查找 root_dir 下所有 .txt 文件（标签文件）。
    返回一个字典：
        key = 文件完整路径（字符串）
        value = ID(加00110) - 便于后续查找
    """
    root = Path(root_dir)
    if not root.exists() or not root.is_dir():
        return {}

    result: Dict[str, str] = {}
    for p in root.rglob("*.txt"):
        if p.is_file():
            basename = p.stem
            parts = basename.split('_')
            ID = parts[0] + '_' + parts[3] + '00110'
            result[ID] = str(p)
    return result

def list_export_dirs(dir_path: str | Path, prefix: str = "export") -> list[str]:
    """
    传入一个目录路径：
      1）先获取该目录下一层所有子目录
      2）在每个子目录下查找名字以 prefix 开头的子目录（例如 export*）
    返回所有这些子目录的完整路径列表（字符串），不再递归更深。
    如果路径不存在或不是目录，返回空列表。
    """
    root = Path(dir_path)
    if not root.exists() or not root.is_dir():
        return []

    result: list[str] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        for sub in child.iterdir():
            if sub.is_dir() and sub.name.startswith(prefix):
                result.append(str(sub))
    return result

import re
from pathlib import Path

def get_id_and_scene_from_export_dir(export_dir: str | Path) -> tuple[str, str]:
    r"""
    根据 export 目录路径推断 ID 和 scene。

    示例：
    D:\...\set2\test_gaming_2026011201\export_Time20260112_001646
        -> ID   = 2026011201 + "00110" = 202601120100110
        -> scene = "gaming"
    """
    p = Path(export_dir)

    # 只用父目录名 test_gaming_2026011201 来解析 scene 和基础 ID
    parent = p.parent
    m = re.match(r"test_([a-zA-Z]+)_(\d{10})$", parent.name)
    if not m:
        raise ValueError(f"无法从父目录名解析 scene/ID：{parent.name}")

    scene = m.group(1)        # gaming
    base_id = m.group(2)      # 2026011201

    # 按规则：ID = base_id + 固定后缀 "00110"
    sample_id = base_id + "00110"

    return sample_id, scene

def format_sample_line(sample: dict) -> str:
    """
    按 address_List.txt 的格式组装一行字符串
    """
    # 对 src_Add 和 lag_timeList_path 加上双引号，其它保持原样
    src_add = f"\"{sample['src_Add']}\"" if sample.get("src_Add") else ""
    lag_path = f"\"{sample['lag_timeList_path']}\"" if sample.get("lag_timeList_path") else ""

    line = (
        f"ID:{sample.get('ID','')},  "
        f"src_Add:{src_add},  "
        f"scene:{sample.get('scene','')},  "
        f"local_ip:{sample.get('local_ip','')},  "
        f"serv_ip:{sample.get('serv_ip','')},  "
        f"start_time:{sample.get('start_time','')},  "
        f"end_time:{sample.get('end_time','')},  "
        f"lag_timeList_path:{lag_path} "
    )
    return line

def parse_sample_line(line: str) -> dict:
    """
    把一行 add_list 样本行解析成字典：
    "ID:...,  src_Add:\"...\" ,  scene:..., ..." -> { 'ID': '...', 'src_Add': '...', ... }
    """
    features = {}
    for item in re.split(r',\s*', line.strip()):
        if ':' not in item:
            continue
        key, value = item.split(':', 1)
        key = key.strip()
        value = value.strip().strip('"')
        features[key] = value
    return features

def _get_sample_line_indices(lines: list[str]) -> list[int]:
    """
    返回所有“样本行”的物理行号列表（0-based），
    样本行定义：去掉首尾空白后不为空、不以 # 开头，并且以 'ID:' 开头。
    """
    indices = []
    for i, raw in enumerate(lines):
        s = raw.strip()
        if not s or s.startswith('#'):
            continue
        if s.startswith('ID:'):
            indices.append(i)
    return indices

def update_field_in_addlist(
    file_path: str | Path,
    sample_index: int,
    field: str,
    value: str,
) -> None:
    """
    修改 add_list 文件中“第 sample_index 条样本”的某个字段。
    - sample_index：按样本顺序计数（忽略空行和 # 行），从 0 开始。
    - field：必须在 FIELDS 里，比如 'src_Add'、'scene' 等。
    """
    if field not in FIELDS:
        raise ValueError(f"未知字段: {field}，允许字段: {FIELDS}")

    file_path = Path(file_path)
    lines = file_path.read_text(encoding='utf-8').splitlines(keepends=True)

    sample_line_indices = _get_sample_line_indices(lines)
    if not (0 <= sample_index < len(sample_line_indices)):
        raise IndexError(f"样本序号 {sample_index} 超出范围，共 {len(sample_line_indices)} 条样本")

    line_no = sample_line_indices[sample_index]
    old_line = lines[line_no].rstrip('\n').rstrip('\r')

    sample = parse_sample_line(old_line)
    sample[field] = value

    new_line = format_sample_line(sample) + '\n'
    lines[line_no] = new_line

    file_path.write_text(''.join(lines), encoding='utf-8')

def append_sample_to_addlist(file_path: str | Path, sample: dict) -> None:
    """
    往已有的 add_list 文件（file_path）末尾追加一条样本行。
    sample 至少需要包含 FIELDS 里的键（缺失的会被填成空串）。
    """
    file_path = Path(file_path)
    sample_full = {k: sample.get(k, "") for k in FIELDS}
    line = format_sample_line(sample_full)
    with file_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def create_new_addlist_file(out_dir: str | Path) -> Path:
    r"""
    在指定目录下（如：'D:\XFC_files\code\UDP2026\config'）新建一个空的 add_list 表，文件名为 address_List-操作时间.txt，
    返回新文件路径。
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"address_List-{ts_str}.txt"
    out_path.write_text("", encoding="utf-8")
    return out_path

if __name__ == "__main__":
    addlist_path = r'D:\XFC_files\code\UDP2026\config\address_List-hset3.txt'
    addlistparser = parser.SampleListParser(addlist_path)
    
    ipv4_dir = r'D:\XFC_files\code\UDP2026\src\pcap\tshark\ipv4_ranks'
    ipv6_dir = r'D:\XFC_files\code\UDP2026\src\pcap\tshark\ipv6_ranks'
    
    new_addlist = create_new_addlist_file(out_dir=r'D:\XFC_files\code\UDP2026\config')
    export_list = list_export_dirs(r"D:\XFC_files\code\UDP2026\data\raw\BBKlog\set2")
    # print(export_list) 
    label_dict = find_label_txt_files(r'D:\XFC_files\code\UDP2026\data_label')
    # print(label_dict)

    for sample in export_list:
        try:
            # sample <--> src_add
            ID, scene = get_id_and_scene_from_export_dir(sample)
            id = scene + '_' + ID[:10]
            local_ip, serv_ip = ip.get_host_server_ip(sample,
                                                    ID,
                                                    scene,
                                                    ipv4_dir,
                                                    ipv6_dir
                                                    )
            start_time = addlistparser.get_sample_info(id)['start_time']
            end_time = addlistparser.get_sample_info(id)['end_time']
            label_path = label_dict[scene + '_' + ID]
            # print(f"{ID}-{scene}")

            sample_dict = {
                "ID": ID,
                "src_Add": sample,          # export 目录路径
                "scene": scene,
                "local_ip": local_ip,
                "serv_ip": serv_ip,
                "start_time": start_time,
                "end_time": end_time,
                "lag_timeList_path": label_path,
            }

            # 写入一行到新的 addlist
            append_sample_to_addlist(new_addlist, sample_dict)
    
        except Exception:
            print(f"\033[31m样本出错: src_Add={sample}, ID={ID}, scene={scene}\033[0m")
            traceback.print_exc()
            # 出错样本直接跳过，不写入 addlist
            continue

    # 竖直对齐字段美化下
    shape.align_fields(new_addlist, new_addlist)