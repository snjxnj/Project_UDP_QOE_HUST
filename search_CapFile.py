import os
import pandas as pd

test_add = "D:\\General_Workspace\\Workspace-of-UDP-NEW\\20250520_UDP\\gaming_wangzhe\\PD2324\\export_Time20250511_125027"

def search_CapFile_in_Dir(src_add = test_add):
    parten = "netlog"
    capFile_Path = ""

    for root, dirs, files in os.walk(src_add):
        if parten in dirs:
            capFile_Path = os.path.join(root, parten)
            break
    return capFile_Path


def search_CapFiles(samples_df):
    result = pd.DataFrame()
    cache = []

    for index, row in samples_df.iterrows():
        samples_ID = row['ID']
        src_Add = row['src_Add']
        scene = row['scene']
        lib_add = row['lib_add']
        local_ip = row['local_ip']
        serv_ip = row['serv_ip']
        start_time = row['start_time']
        end_time = row['end_time']
        lag_timeList_path = row['lag_timeList_path']

        capFile_add = ""
        capFile_add = search_CapFile_in_Dir(src_Add)
        if capFile_add.__sizeof__() == 0:
            print("not found!")
            capFile_add = "---"
        cache.append({
            'samples_ID': samples_ID,
            'src_Add': src_Add,
            'scene': scene,
            'lib_add': lib_add,
            'local_ip': local_ip,
            'serv_ip': serv_ip,
            'start_time': start_time,
            'end_time': end_time,
            'capFile_add': capFile_add,
            'lag_timeList_path': lag_timeList_path,
        })
    result = pd.DataFrame(cache)
    return result

search_CapFile_in_Dir()