import os
import time
import pandas as pd
import numpy as np
import sys

def isFileExists(filepath):
    return os.path.isfile(filepath)

def isDirExists(dirpath):
    return os.path.isdir(dirpath)

def extract_from_oneGroup(send_file, recv_file, output_dir = '.\\'):
    # 打印输出，检测文件和目录的合法性
    # print(f"file name: {send_file}, is it a file: {isFileExists(send_file)}")
    # print(f"file name: {recv_file}, is it a file: {isFileExists(recv_file)}")
    # print(f"dir name: {output_dir}, is it a dir: {isDirExists(output_dir)}")
    
    # 合法性检测
    if(not isFileExists(send_file) 
        or not isFileExists(recv_file) 
        or not isDirExists(output_dir)):
        print('fatal error: some file or dir disappear!')
        return -1
    # print(send_file);print(recv_file);print(output_dir)

    # 加载数据信息
    send_df = pd.read_csv(send_file)
    recv_df = pd.read_csv(recv_file)
    # 打印数据信息以备检测
    # print(f"send_df: {send_df.head()}")
    # print(f"recv_df: {recv_df.head()}")

    # 对send_df进行遍历，获取包间隔信息
    pre_time = send_df.loc[send_df.index[0], 'frame.time_epoch']
    send_frame_interval = []
    for index, row in send_df.iterrows():
        # 缓存当前时间戳
        current_time = row['frame.time_epoch']
        # 计算当前包与前一包的包间隔
        internal = current_time - pre_time
        # 更新上一包的时间戳
        pre_time = current_time
        # 将当前包与上一包的包间隔存储列表信息中
        send_frame_interval.append(internal)
    # 将包间隔信息存入
    send_df['frame_interval'] = send_frame_interval
    # 打印数据信息以备检测
    # print(f"send_df: {send_df.head()}")

    # 对recv_df进行遍历，获取包间隔信息
    pre_time = recv_df.loc[recv_df.index[0], 'frame.time_epoch']
    recv_frame_interval = []
    for index, row in recv_df.iterrows():
        # 缓存当前时间戳
        current_time = row['frame.time_epoch']
        # 计算当前包与前一包的包间隔
        internal = current_time - pre_time
        # 更新上一包的时间戳
        pre_time = current_time
        # 将当前包与上一包的包间隔存储列表信息中
        recv_frame_interval.append(internal)
    # 将包间隔信息存入
    recv_df['frame_interval'] = recv_frame_interval
    # 打印数据信息以备检测
    # print(f"recv_df: {recv_df.head()}")

    # 确定样本采集区间
    """
    已知所有的收包信息和发包信息，我们将会按照1s窗口长度进行采样
    现在我们要确定，整个的实验时间区间是什么，从而进一步将区间划分为1s间隔的窗口序列
    采用标准：
        实验开始时间：
            收包和发包信息当中，最早的一条信息的时间戳的向下取整
        实验结束时间：
            收包和发包消息当中，最晚的一条信息的时间戳的向下取整
    """
    min_time = int(min(send_df['frame.time_epoch'].min(), recv_df['frame.time_epoch'].max()))
    max_time = int(max(send_df['frame.time_epoch'].max(), recv_df['frame.time_epoch'].max()))

    # 创建空列表以缓存从窗口当中提取的时间
    features_of_allWindows = []
    """
    假设，11-12s没有任何发包和收包，那么1s窗口采集到该区间时，
    会发现窗口中的各项特征值为空，则在该窗口内，算法无法统计平均、
    最大、最小等诸多信息，我们可以设置为默认值1s，因为窗口区间长
    度就是1s，但是这样将会丢失部分重要信息。
    所以我们的一致解决方案时：
        算法会记录最近的一个包信息，记录方式便是，没扫描一个窗口
    算法就会记录下窗口当中最后一个有效包信息，如果窗口当中没有有
    效的包信息，那么算法将会利用此前记录的最后一个有效包信息来进行
    信息更新。
        这个用来记录已扫描信息当中最后一个有效包信息的变量名为，
    last_Valid_PacketTime_fromSendDF
    last_Valid_PacketTime_fromRecvDF
    """
    last_Valid_PacketTime_fromSendDF = send_df.loc[send_df.index[0], 'frame.time_epoch']
    last_Valid_PacketTime_fromRecvDF = recv_df.loc[recv_df.index[0], 'frame.time_epoch']

    # 开始遍历recv_df和send_df当中每一个1s窗口中的内容
    for startTime_of_curWindow in np.arange(min_time, max_time + 1, 1.0):
        # 将Unix时间戳信息转换为北京时间，便于后续比较
        curTime_of_UTC8 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(startTime_of_curWindow))
        
        # 从send_df和recv_df当中采集当前窗口区间中包含的信息
        sendDF_in_curWindow = send_df[(send_df['frame.time_epoch'] >= startTime_of_curWindow) &
                                        (send_df['frame.time_epoch'] < startTime_of_curWindow + 1)]
        recvDF_in_curWindow = recv_df[(recv_df['frame.time_epoch'] >= startTime_of_curWindow) &
                                        (recv_df['frame.time_epoch'] < startTime_of_curWindow + 1)]
        # 计算各类特征
        # 计算当前窗口内收发的数据包数量
        num_send_packets = len(sendDF_in_curWindow)
        num_recv_packets = len(recvDF_in_curWindow)
        # 计算当前窗口内收发的平均包字节量
        avg_send_packetLen = sendDF_in_curWindow['frame.len'].mean() if not sendDF_in_curWindow.empty else 0
        avg_recv_packetLen = recvDF_in_curWindow['frame.len'].mean() if not recvDF_in_curWindow.empty else 0
        # 计算当前窗口收发包的总字节流量
        send_dataStream = sendDF_in_curWindow['frame.len'].sum()
        recv_dataStream = recvDF_in_curWindow['frame.len'].sum()
        # 计算当前窗口内收发包的平均、最大、最小包间隔以及包间隔的标准差
        if not sendDF_in_curWindow.empty:
            # 首先更新最后一个有效包的信息
            last_Valid_PacketTime_fromSendDF = sendDF_in_curWindow.iloc[-1]['frame.time_epoch']
            # 计算当前窗口内发包的各项指标
            avg_send_packetInterval = sendDF_in_curWindow['frame_interval'].mean()
            max_send_packetInterval = sendDF_in_curWindow['frame_interval'].max()
            min_send_packetInterval = sendDF_in_curWindow['frame_interval'].min()
            std_send_packetInterval = sendDF_in_curWindow['frame_interval'].std() if not (sendDF_in_curWindow.index.size == 1) else 0
            cv_send_packetInterval = std_send_packetInterval / avg_send_packetInterval if avg_send_packetInterval != 0 else 0
        else:
            avg_send_packetInterval = startTime_of_curWindow + 1 - last_Valid_PacketTime_fromSendDF
            max_send_packetInterval = startTime_of_curWindow + 1 - last_Valid_PacketTime_fromSendDF
            min_send_packetInterval = startTime_of_curWindow + 1 - last_Valid_PacketTime_fromSendDF
            std_send_packetInterval = 0
            cv_send_packetInterval = 0
        # 计算当前窗口内收包的平均、最大、最小包间隔以及包间隔的标准差
        if not recvDF_in_curWindow.empty:
            # 首先更新最后一个有效包的信息
            last_Valid_PacketTime_fromRecvDF = recvDF_in_curWindow.iloc[-1]['frame.time_epoch']
            # 计算当前窗口内收包的各项指标
            avg_recv_packetInterval = recvDF_in_curWindow['frame_interval'].mean()
            max_recv_packetInterval = recvDF_in_curWindow['frame_interval'].max()
            min_recv_packetInterval = recvDF_in_curWindow['frame_interval'].min()
            std_recv_packetInterval = recvDF_in_curWindow['frame_interval'].std() if not (recvDF_in_curWindow.index.size == 1) else 0
            cv_recv_packetInterval = std_recv_packetInterval / avg_recv_packetInterval if avg_recv_packetInterval != 0 else 0
        else:
            avg_recv_packetInterval = startTime_of_curWindow + 1 - last_Valid_PacketTime_fromRecvDF
            max_recv_packetInterval = startTime_of_curWindow + 1 - last_Valid_PacketTime_fromRecvDF
            min_recv_packetInterval = startTime_of_curWindow + 1 - last_Valid_PacketTime_fromRecvDF
            std_recv_packetInterval = 0
            cv_recv_packetInterval = 0
        # 计算突变率
        if len(features_of_allWindows) == 0:
            send_Mutation_of_numPackets = -1
            recv_Mutation_of_numPackets = -1
        else:
            pre_num_send_packets = features_of_allWindows[-1]['num_send_packets']
            pre_num_recv_packets = features_of_allWindows[-1]['num_recv_packets']
            send_Mutation_of_numPackets = (num_send_packets - pre_num_send_packets) / pre_num_send_packets if pre_num_send_packets != 0 else 0
            recv_Mutation_of_numPackets = (num_recv_packets - pre_num_recv_packets) / pre_num_recv_packets if pre_num_recv_packets != 0 else 0
        # 汇总该窗口内的所有特征为一个字典，并将这些特征添加到存储所有窗口信息的列表当中
        features_of_curWindow = {
            'curTime_of_UTC8': curTime_of_UTC8,
            'curWindow': startTime_of_curWindow,
            'num_send_packets': num_send_packets,
            'avg_send_packetLen': avg_send_packetLen,
            'send_dataStream': send_dataStream,
            'avg_send_packetInterval': avg_send_packetInterval,
            'max_send_packetInterval': max_send_packetInterval,
            'min_send_packetInterval': min_send_packetInterval,
            'std_send_packetInterval': std_send_packetInterval,
            'cv_send_packetInterval': cv_send_packetInterval,
            'send_Mutation_of_numPackets': send_Mutation_of_numPackets,
            'num_recv_packets': num_recv_packets,
            'avg_recv_packetLen': avg_recv_packetLen,
            'recv_dataStream': recv_dataStream,
            'avg_recv_packetInterval': avg_recv_packetInterval,
            'max_recv_packetInterval': max_recv_packetInterval,
            'min_recv_packetInterval': min_recv_packetInterval,
            'std_recv_packetInterval': std_recv_packetInterval,
            'cv_recv_packetInterval': cv_recv_packetInterval,
            'recv_Mutation_of_numPackets': recv_Mutation_of_numPackets,
        }
        features_of_allWindows.append(features_of_curWindow)
    
    #将存储所有窗口信息的字典列表转换为一个DataFrame数据结构
    df_features_of_allWindows = pd.DataFrame(features_of_allWindows)
    # 将该信息打印以备检测
    # print(df_features_of_allWindows)

    # 将所有窗口的汇总信息打印为csv文件，且不保存索引
    df_features_of_allWindows.to_csv(output_dir + '\\' + 'extracted_UDP_features.csv', index=False)






"""
@brief 从一个分组中提取UDP特征
@param send_file 发送数据包的CSV文件路径
@param recv_file 接收数据包的CSV文件路径
@param output_dir 输出特征的目录
@return 无
@attention 该版本由于陈旧已经舍弃
"""
def extract_from_oneGroup_oldVersion(send_file, recv_file, output_dir = '.\\'):
    # 加载数据，将发送和接收数据包的CSV文件读取为DataFrame对象
    # 加载数据
    send_df = pd.read_csv(send_file)
    recv_df = pd.read_csv(recv_file)
    # delay_df = pd.read_csv(merged_logs_file)

    ### 进行send_df的逐行遍历
    pre_time = send_df.loc[send_df.index[0], 'frame.time_epoch']
    now_time = 0
    delays = []
    for index, row in send_df.iterrows():
        # 这里可以添加对每行数据的处理逻辑
        # print(f"Index: {index}, 数据: {row}")
        now_time = row['frame.time_epoch']
        # 计算当前秒内的延迟
        delay = now_time - pre_time
        pre_time = now_time
        # 将本次计算的时延加入列表中
        delays.append(delay)
    
    send_df['frame_delay'] = delays
    ###

    ### 进行recv_df的逐行遍历
    # 对recv_df进行逐行遍历
    pre_time = recv_df.loc[recv_df.index[0], 'frame.time_epoch']
    now_time = 0
    delays = []
    for index, row in recv_df.iterrows():
        # 这里可以添加对每行数据的处理逻辑
        # print(f"Index: {index}, 数据: {row}")
        now_time = row['frame.time_epoch']
        # 计算当前秒内的延迟
        delay = now_time - pre_time
        pre_time = now_time
        # 将本次计算的时延加入列表中
        delays.append(delay)

    recv_df['frame_delay'] = delays
    ###

    # 确保最小和最大的时间戳以确定范围
    # 找出发送和接收数据中时间戳的最小值，并转换为整数
    min_time = int(min(send_df['frame.time_epoch'].min(), recv_df['frame.time_epoch'].min()))
    # 找出发送和接收数据中时间戳的最大值，并转换为整数
    max_time = int(max(send_df['frame.time_epoch'].max(), recv_df['frame.time_epoch'].max()))

    last_delay_100 = 0
    last_delay_200 = 0
    last_delay = 0
    last_second_tail = 0
    features = []
    # 用于为下一秒的数据处理存储上一秒的最有一个数据包的时间
    send_tailTime_of_last_second = send_df.loc[send_df.index[0], 'frame.time_epoch']
    recv_tailTime_of_last_second = recv_df.loc[recv_df.index[0], 'frame.time_epoch']
    # 使用numpy的arange函数，以1秒为步长，遍历从最小时间戳到最大时间戳的每一秒
    for timestamp in np.arange(min_time, max_time + 1, 1.0):  # 使用步长为1.0遍历每一秒
        # 过滤出当前秒内发送和接收的数据包数据
        # 过滤出当前秒的数据
        send_second_df = send_df[(send_df['frame.time_epoch'] >= timestamp) & (send_df['frame.time_epoch'] < timestamp + 1)]
        recv_second_df = recv_df[(recv_df['frame.time_epoch'] >= timestamp) & (recv_df['frame.time_epoch'] < timestamp + 1)]

        # 计算各种特征
        # 计算当前秒内发送的数据包数量
        avg_send_packets = len(send_second_df)
        # 计算当前秒内接收的数据包数量
        avg_recv_packets = len(recv_second_df)
        # 计算当前秒内发送数据包的平均长度，如果没有数据则为0
        avg_send_len = send_second_df['frame.len'].astype(int).mean() if not send_second_df.empty else 0
        # 计算当前秒内接收数据包的平均长度，如果没有数据则为0
        avg_recv_len = recv_second_df['frame.len'].astype(int).mean() if not recv_second_df.empty else 0
        
        # 统计窗口内的数据流量情况
        send_total_data_stream = send_second_df['frame.len'].astype(int).sum()
        recv_total_data_stream = recv_second_df['frame.len'].astype(int).sum()

        # 计算包间隔
        # 如果当前秒内有发送数据包的数据
        if not send_second_df.empty:
            # 缓存当前1s中的最后一个数据包时间，为未来做好准备
            send_tailTime_of_last_second = send_second_df.iloc[-1]['frame.time_epoch']
            # 由于向前差分信号已经记录完毕，所以在不为空情况下进行直接计算
            avg_frame_interval_send = send_second_df['frame_delay'].mean()
            max_frame_interval_send = send_second_df['frame_delay'].max()
            min_frame_interval_send = send_second_df['frame_delay'].min()
            std_frame_interval_send = send_second_df['frame_delay'].std()
        else:
            avg_frame_interval_send = timestamp + 1 - send_tailTime_of_last_second
            max_frame_interval_send = timestamp + 1 - send_tailTime_of_last_second
            min_frame_interval_send = timestamp + 1 - send_tailTime_of_last_second
            std_frame_interval_send = 0

        # 如果当前秒内有接收数据包的数据
        if not recv_second_df.empty:
            # 缓存当前1s中的最后一个数据包时间，为未来做好准备
            recv_tailTime_of_last_second = recv_second_df.iloc[-1]['frame.time_epoch']
            # 由于向前差分信号已经记录完毕，所以在不为空情况下进行直接计算
            avg_frame_interval_recv = recv_second_df['frame_delay'].mean()
            max_frame_interval_recv = recv_second_df['frame_delay'].max()
            min_frame_interval_recv = recv_second_df['frame_delay'].min()
            std_frame_interval_recv = recv_second_df['frame_delay'].std()
        else:
            avg_frame_interval_recv = timestamp + 1 - recv_tailTime_of_last_second
            max_frame_interval_recv = timestamp + 1 - recv_tailTime_of_last_second
            min_frame_interval_recv = timestamp + 1 - recv_tailTime_of_last_second
            std_frame_interval_recv = 0

        # 100ms分桶统计
        # 生成一个列表，将每秒分成10个0.1秒的区间，用于直方图统计
        bins = [timestamp + i * 0.1 for i in range(11)]  # 每秒分成10个0.1秒的区间
        # 统计发送数据包在每个0.1秒区间内的数量
        send_counts, _ = np.histogram(send_second_df['frame.time_epoch'], bins=bins)
        # 统计接收数据包在每个0.1秒区间内的数量
        recv_counts, _ = np.histogram(recv_second_df['frame.time_epoch'], bins=bins)
        
        # 计算发送数据包在0.1秒区间内数量为0的比例
        zero_pkt_100ms_send = np.mean(send_counts == 0)
        # 计算发送数据包在0.1秒区间内数量为1的比例
        one_pkt_100ms_send = np.mean(send_counts == 1)
        # 计算发送数据包在0.1秒区间内数量大于4的比例
        more_than_four_pkt_100ms_send = np.mean(send_counts > 4)
        # 计算接收数据包在0.1秒区间内数量为0的比例
        zero_pkt_100ms_recv = np.mean(recv_counts == 0)
        # 计算接收数据包在0.1秒区间内数量为1的比例
        one_pkt_100ms_recv = np.mean(recv_counts == 1)
        # 计算接收数据包在0.1秒区间内数量大于4的比例
        more_than_four_pkt_100ms_recv = np.mean(recv_counts > 4)

        # 计算接收数据包数量与发送数据包数量的比例，如果发送数据包数量为0则为0
        packet_ratio = avg_recv_packets / avg_send_packets if avg_send_packets != 0 else 0
        # 计算接收数据包总长度与发送数据包总长度的比例，如果发送数据包总长度为0则为0
        byte_ratio = avg_recv_len / avg_send_len if avg_send_len != 0 else 0

        # 计算突变率
        if len(features) == 0:
            mutation_rate_send = -1
            mutation_rate_recv = -1
        else:
            prev_avg_send = features[-1]['avg_send_packets']
            prev_avg_recv = features[-1]['avg_recv_packets']
            mutation_rate_send = (avg_send_packets - prev_avg_send) / avg_send_packets if avg_send_packets != 0 else 0
            mutation_rate_recv = (avg_recv_packets - prev_avg_recv) / avg_recv_packets if avg_recv_packets != 0 else 0

        # 定义一个字典，存储当前秒的所有特征
        feature = {
            'timestamp': timestamp,
            'avg_send_packets': avg_send_packets,
            'avg_recv_packets': avg_recv_packets,
            'avg_send_len': avg_send_len,
            'avg_recv_len': avg_recv_len,
            'avg_frame_interval_send': avg_frame_interval_send,
            'max_frame_interval_send': max_frame_interval_send,
            'min_frame_interval_send': min_frame_interval_send,
            'std_frame_interval_send': std_frame_interval_send,
            'send_total_data_stream': send_total_data_stream,
            'avg_frame_interval_recv': avg_frame_interval_recv,
            'max_frame_interval_recv': max_frame_interval_recv,
            'min_frame_interval_recv': min_frame_interval_recv,
            'std_frame_interval_recv': std_frame_interval_recv,
            'recv_total_data_stream': recv_total_data_stream,
            'zero_pkt_100ms_send': zero_pkt_100ms_send,
            'one_pkt_100ms_send': one_pkt_100ms_send,
            'more_than_four_pkt_100ms_send': more_than_four_pkt_100ms_send,
            'zero_pkt_100ms_recv': zero_pkt_100ms_recv,
            'one_pkt_100ms_recv': one_pkt_100ms_recv,
            'more_than_four_pkt_100ms_recv': more_than_four_pkt_100ms_recv,
            # 'delay_gt_100': delay_100,
            # 'delay_gt_200': delay_200,
            # 'delay': delay_value,
            'packet_ratio': packet_ratio,
            'byte_ratio': byte_ratio,
            'mutation_rate_send': mutation_rate_send,
            'mutation_rate_recv': mutation_rate_recv
            # 'ip.src': ','.join(pd.concat([send_second_df['ip.src'], recv_second_df['ip.src']]).unique()),
            # 'ip.dst': ','.join(pd.concat([send_second_df['ip.dst'], recv_second_df['ip.dst']]).unique())
        }
        features.append(feature)

    # 保存到CSV
    # 将特征列表转换为pandas的DataFrame对象
    df_features = pd.DataFrame(features)
    # 将特征DataFrame保存为CSV文件，不保存索引
    df_features.to_csv(os.path.join(output_dir, 'extracted_data.csv'), index=False)


# 2025.9.1 测试示例
if __name__ == '__main__':
    # 从命令行获取参数
    # 第一个参数是source_path，第二个参数是target_path
    if len(sys.argv) < 3:
        print("使用方法: python extract_UDP_features.py <source_path> <target_path>")
        print("示例: python extract_UDP_features.py ./input_dir ./output_dir")
        sys.exit(1)
    
    # 获取命令行参数
    source_path = sys.argv[1]
    target_path = sys.argv[2]
    
    # 确保路径以反斜杠结尾
    if not source_path.endswith('\\') and not source_path.endswith('/'):
        source_path += '\\'
    
    if not target_path.endswith('\\') and not target_path.endswith('/'):
        target_path += '\\'
    
    # 检查目录是否存在
    if not os.path.isdir(source_path):
        print(f"错误: 源目录 '{source_path}' 不存在")
        sys.exit(1)
    
    # 确保输出目录存在
    if not os.path.isdir(target_path):
        os.makedirs(target_path)
        print(f"创建输出目录: {target_path}")
    
    # 查找send和recv文件
    send_files = [f for f in os.listdir(source_path) if f.endswith('_send.csv')]
    recv_files = [f for f in os.listdir(source_path) if f.endswith('_recv.csv')]
    
    if not send_files or not recv_files:
        print(f"错误: 在源目录 '{source_path}' 中找不到send.csv或recv.csv文件")
        sys.exit(1)
    
    # 匹配send和recv文件对
    # 假设文件名格式为xxx_send.csv和xxx_recv.csv
    file_pairs = []
    for send_file in send_files:
        # 获取基础文件名（去掉_send.csv后缀）
        base_name = send_file[:-9]  # 移除_send.csv
        # 查找对应的recv文件
        corresponding_recv = base_name + '_recv.csv'
        if corresponding_recv in recv_files:
            file_pairs.append((os.path.join(source_path, send_file), os.path.join(source_path, corresponding_recv)))
    
    if not file_pairs:
        print("错误: 找不到匹配的send和recv文件对")
        sys.exit(1)
    
    print(f"找到 {len(file_pairs)} 对文件")
    
    # 处理每一对文件
    for i, (send_file, recv_file) in enumerate(file_pairs, 1):
        print(f"处理文件对 {i}/{len(file_pairs)}: {os.path.basename(send_file)} 和 {os.path.basename(recv_file)}")
        result = extract_from_oneGroup(send_file, recv_file, target_path)
        if result == -1:
            print(f"处理文件对 {i} 失败")
        else:
            print(f"文件对 {i} 处理成功")
    
    print(f"特征提取完成，结果保存在: {target_path}")
