import os
import pandas as pd
from datetime import datetime as dt, timedelta, time

"""
2024.04.09
@brief  read_lagList_v1函数，读取2025版本lagTimeList
@param  lag_path 卡顿区间列表的地址信息
@return DF数据结构，记录区间及其开始截至时间
"""
def read_lagList_v1(lag_path: str) -> pd.DataFrame:
    # 从文件名中提取日期信息
    # 文件名格式: video_dbad_sgood_2025103101_lag_timeList.txt
    # 去掉最后2位得到日期: 20251031
    filename = os.path.basename(lag_path)
    parts = filename.split('_')
    date_str = parts[3][:-2]  # 得到 20251031
    base_date = dt.strptime(date_str, "%Y%m%d").date()
    
    start_times = []
    end_times = []
    current_date = base_date
    
    with open(lag_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or '-' not in line:
                continue
            
            # 解析时间区间，格式: HH:MM:SS.mmm-HH:MM:SS.mmm
            start_str, end_str = line.split('-', 1)
            start_str = start_str.strip()
            end_str = end_str.strip()
            
            # 解析开始时间 (时分秒.毫秒)
            start_parts = start_str.split(':')
            start_hour = int(start_parts[0])
            start_min = int(start_parts[1])
            start_sec_ms = start_parts[2].split('.')
            start_sec = int(start_sec_ms[0])
            start_ms = int(start_sec_ms[1])
            
            # 解析结束时间
            end_parts = end_str.split(':')
            end_hour = int(end_parts[0])
            end_min = int(end_parts[1])
            end_sec_ms = end_parts[2].split('.')
            end_sec = int(end_sec_ms[0])
            end_ms = int(end_sec_ms[1])
            
            # 判断是否跨越到第二天
            start_time_obj = time(start_hour, start_min, start_sec, start_ms * 1000)
            end_time_obj = time(end_hour, end_min, end_sec, end_ms * 1000)
            
            if end_time_obj < start_time_obj:
                current_date = base_date + timedelta(days=1)
            else:
                current_date = base_date
            
            # 构建完整的datetime对象
            start_datetime = dt(
                current_date.year, current_date.month, current_date.day,
                start_hour, start_min, start_sec, start_ms * 1000
            )
            end_datetime = dt(
                current_date.year, current_date.month, current_date.day,
                end_hour, end_min, end_sec, end_ms * 1000
            )
            
            start_times.append(start_datetime)
            end_times.append(end_datetime)
    
    return pd.DataFrame({
        'lag_startTime': start_times,
        'lag_endTime': end_times
    })


if __name__ == "__main__":
    lag_path = r"D:\General_Workspace\Workspace-of-UDP-NEW\Workspace-for-StableEnvironment\Flow_Analysis\test\test_lagList\video_dbad_sgood_2025103101_lag_timeList.txt"
    opt_dir = r"D:\General_Workspace\Workspace-of-UDP-NEW\Workspace-for-StableEnvironment\Flow_Analysis\test\test_lagList"
    result = pd.DataFrame()
    result = read_lagList_v1(lag_path)
    
    # 导出为csv文件
    csv_path = os.path.join(opt_dir, "result.csv")
    result.to_csv(csv_path, index=False, date_format='%Y-%m-%d %H-%M-%S.%f')
