"""
比较实际数据与预测数据，进行容错处理的多种方案，计算准确率和召回率
文件中有多个函数可以直接使用，每个函数实现一种比对方案：
    1. calculate_metrics: 直接比对法
    2. sliding_window_tolerance: 滑动窗口容错法
    3. edge_tolerance_matching: 边缘容错法
    4. segment_matching: 卡顿时段匹配度法
    以及两个辅助函数：
    - _find_segments: 找出卡顿时段
    - _calculate_overlap: 计算两个时段的重叠度
    还有一个计算准确率和召回率的函数：calculate_seq_metrics
在使用每个函数时，请先阅读函数注释，了解参数和返回值。
最后，代码中还包含一个示例用法，展示如何加载数据文件并调用各个函数进行比较。
    参数说明：
        qoe_data: 实际数据序列 (列表或numpy数组，卡顿为 1，非卡顿为 0)
        calcu_data: 预测数据序列 (列表或numpy数组，卡顿为 1，非卡顿为 0)
        window_size: 滑动窗口大小 (正奇数)
        threshold: 容错阈值 (0 到 1 之间)
        tolerance: 容错时间范围 (正整数)
    返回值说明：
        accuracy: 准确率 (浮点数)
        recall: 召回率 (浮点数)
        seq_accuracy: 准确序列 (numpy数组，1表示预测准确，0表示预测不准确)
"""

import numpy as np

# 计算准确率和召回率的函数
def calculate_metrics(qoe_data, calcu_data):
    """计算准确率和召回率
    参数：    qoe_data: 实际数据序列
            calcu_data: 预测数据序列
    返回值：    accuracy: 准确率
            recall: 召回率
            seq_accuracy: 准确序列
    """
    # 确保输入为numpy数组类型
    qoe_data = np.array(qoe_data)
    calcu_data = np.array(calcu_data)

    # 计算整个序列的准确率
    accuracy = np.sum(qoe_data == calcu_data) / len(qoe_data)

    # 计算召回率（针对卡顿时段）
    recall = np.sum((qoe_data == 1) & (calcu_data == 1)) / np.sum(qoe_data == 1)

    # 计算准确识别的序列（预测准确为1，不准确为0）
    seq_accuracy = np.where(qoe_data == calcu_data, 1, 0)

    return accuracy, recall, seq_accuracy


def sliding_window_tolerance(qoe_data, calcu_data, window_size=3, threshold=0.5):
    """使用滑动窗口容错法计算准确度
    参数：    qoe_data: 实际数据序列
            calcu_data: 预测数据序列
            window_size: 滑动窗口大小
            threshold: 容错阈值
    返回：   seq_accuracy: 准确序列
    """
    # 确保输入为numpy数组类型
    qoe_data = np.array(qoe_data)
    calcu_data = np.array(calcu_data)
    # 初始化准确序列
    seq_accuracy = np.zeros_like(qoe_data)
    # 初始化计数器
    total_windows = len(qoe_data) - window_size + 1 # 窗口数量

    for i in range(total_windows):
        # 获取窗口内的真实数据和预测数据
        window_qoe = qoe_data[i:i + window_size]
        window_calcu = calcu_data[i:i + window_size]

        # 计算窗口内的匹配程度
        matches = np.sum(window_qoe == window_calcu)
        # 记录到准确序列中
        center_index = i + (window_size - 1) // 2
        if matches / window_size >= threshold:
            seq_accuracy[center_index] = 1  # 中心位置标记为正确
        else:
            seq_accuracy[center_index] = 0  # 中心位置标记为错误

    # 边缘处理：填充起始和末尾未覆盖的部分
    edge_size = (window_size - 1) // 2
    seq_accuracy[:edge_size] = seq_accuracy[edge_size]
    seq_accuracy[-edge_size:] = seq_accuracy[-edge_size - 1]
    # 计算准确率
    accuracy = np.mean(seq_accuracy)
    print("滑动窗口准确率：", accuracy)
    # 返回准确序列
    return seq_accuracy


def edge_tolerance_matching(qoe_data, calcu_data, tolerance=1):
    """使用边缘模糊匹配法计算准确度
    参数：    qoe_data: 实际数据序列
            calcu_data: 预测数据序列
            tolerance: 容错阈值
    返回：    seq_accuracy: 准确序列
    """
    # 确保输入为numpy数组类型
    qoe_data = np.array(qoe_data)
    calcu_data = np.array(calcu_data)
    # 初始化准确序列
    seq_accuracy = np.zeros_like(qoe_data)
    total_predictions = len(qoe_data)   # 总预测数量

    for i in range(total_predictions):  # 遍历所有预测
        # 在时间上进行边缘模糊匹配
        if calcu_data[i] == qoe_data[i]:
            seq_accuracy[i] = 1  # 预测正确
        else:
            matched = False
            # 向前容错匹配
            for t in range(1, tolerance + 1):
                if i - t >= 0 and calcu_data[i] == qoe_data[i - t]:
                    seq_accuracy[i] = 1
                    matched = True
                    break
            # 向后容错匹配
            if not matched:
                for t in range(1, tolerance + 1):
                    if i + t < total_predictions and calcu_data[i] == qoe_data[i + t]:
                        seq_accuracy[i] = 1
                        matched = True
                        break
            # 如果没有匹配成功
            if not matched:
                seq_accuracy[i] = 0

    # 计算准确率
    accuracy = np.mean(seq_accuracy)
    print("边缘模糊匹配准确率：", accuracy)
    return seq_accuracy


def segment_matching(qoe_data, calcu_data, threshold=0.5):
    """使用卡顿时段匹配度计算准确度
    参数：    qoe_data: 实际数据序列 (卡顿为 1，非卡顿为 0)
            calcu_data: 预测数据序列 (卡顿为 1，非卡顿为 0)
            threshold: 重叠度阈值 (单向重叠度 IoR)
    返回：    seq_accuracy: 准确序列
    """
    # 确保输入为numpy数组类型
    qoe_data = np.array(qoe_data)
    calcu_data = np.array(calcu_data)
    # 初始化准确序列 (用于点级别平均)
    seq_accuracy = np.zeros(len(calcu_data))

    # 找到所有的卡顿时段
    qoe_segments = _find_segments(qoe_data)
    calcu_segments = _find_segments(calcu_data)

    # print("实际卡顿时段：", qoe_segments)
    # print("预测卡顿时段：", calcu_segments)

    num_qoe = len(qoe_segments)
    num_calcu = len(calcu_segments)

    # 1. 使用动态规划进行卡顿时段匹配
    # dp[i][j] 存储匹配 qoe_segments[:i] 和 calcu_segments[:j] 的最小代价
    dp = np.zeros((num_qoe + 1, num_calcu + 1))

    # 动态规划初始化 (插入/删除的代价为 1)
    for i in range(1, num_qoe + 1):
        dp[i][0] = i  # 全部删除实际卡顿时段
    for j in range(1, num_calcu + 1):
        dp[0][j] = j  # 全部插入预测卡顿时段

    # 动态规划填表
    for i in range(1, num_qoe + 1):  # 实际卡顿时段
        for j in range(1, num_calcu + 1):  # 预测卡顿时段
            # 计算重叠度 (单向：预测对实际的覆盖率)
            overlap = _calculate_overlap(qoe_segments[i - 1], calcu_segments[j - 1])
            cost = 1 - overlap  # 重叠度越高，代价越低 (0 到 1)

            # 选择最小代价的匹配：
            # 1. 删除 qoe[i-1] (+1)
            # 2. 插入 calcu[j-1] (+1)
            # 3. 匹配 qoe[i-1] 和 calcu[j-1] (+cost)
            dp[i][j] = min(dp[i - 1][j] + 1,  # Delete
                           dp[i][j - 1] + 1,  # Insert
                           dp[i - 1][j - 1] + cost)  # Match

    # 2. 回溯找到最佳匹配并计算点准确序列
    i, j = num_qoe, num_calcu
    matched_qoe_segments = 0  # 统计成功匹配的实际卡顿时段数量

    while i > 0 or j > 0:
        if i > 0 and j > 0:
            overlap = _calculate_overlap(qoe_segments[i - 1], calcu_segments[j - 1])
            cost = 1 - overlap

            # 匹配 (Match)
            if np.isclose(dp[i][j], dp[i - 1][j - 1] + cost):
                # 如果是匹配路径，并且重叠度满足阈值，则视为点准确
                if overlap >= threshold:
                    matched_qoe_segments += 1
                    start, end = qoe_segments[i - 1]
                    # 标记实际卡顿时段对应的区间为准确
                    seq_accuracy[start:end + 1] = 1

                i -= 1
                j -= 1
            # 删除 (Delete)
            elif np.isclose(dp[i][j], dp[i - 1][j] + 1):
                i -= 1
            # 插入 (Insert)
            else:  # np.isclose(dp[i][j], dp[i][j - 1] + 1):
                j -= 1
        elif i > 0:  # 仅剩实际卡顿时段 (删除)
            i -= 1
        elif j > 0:  # 仅剩预测卡顿时段 (插入)
            j -= 1

    # 3. 对于非卡顿时段，使用直接比对法
    # 标记实际和预测都是 0 的点为准确
    for k in range(len(calcu_data)):
        if qoe_data[k] == 0 and calcu_data[k] == 0:
            seq_accuracy[k] = 1
        elif qoe_data[k] == 1 and calcu_data[k] == 1:
            seq_accuracy[k] = 1

    # 4. 计算最终准确率
    accuracy = np.mean(seq_accuracy)
    print("卡顿时段匹配准确率：", accuracy)

    return seq_accuracy


# def weighted_segment_matching(qoe_data, calcu_data, long_weight=2, short_weight=1):
#     """使用加权时段比对法计算准确度"""
#     qoe_data = np.array(qoe_data)
#     calcu_data = np.array(calcu_data)
#
#     correct_predictions = 0
#     total_weight = 0
#
#     # 找到所有的卡顿时段
#     qoe_segments = _find_segments(qoe_data)
#     calcu_segments = _find_segments(calcu_data)
#
#     for qoe_seg, calcu_seg in zip(qoe_segments, calcu_segments):
#         overlap = _calculate_overlap(qoe_seg, calcu_seg)
#         segment_length = len(qoe_seg)
#
#         # 长时间卡顿赋予更高的权重
#         weight = long_weight if segment_length >= 3 else short_weight
#         correct_predictions += weight * overlap
#         total_weight += weight
#
#     accuracy = correct_predictions / total_weight
#     return accuracy


# 辅助函数：找出卡顿时段
def _find_segments(data):
    """辅助函数：找出卡顿时段 (值为 1 的连续区间)"""
    segments = []
    current_segment_start = -1
    in_segment = False

    for i in range(len(data)):
        # 假设卡顿为 1
        if data[i] == 1:
            if not in_segment:
                current_segment_start = i
                in_segment = True
        # 如果当前数据不是卡顿 (0) 且存在正在处理的卡顿时段
        elif in_segment:
            segments.append([current_segment_start, i - 1])
            in_segment = False

    # 处理最后一段卡顿
    if in_segment:
        segments.append([current_segment_start, len(data) - 1])

    return segments


# 辅助函数：计算两个时段的重叠度
def _calculate_overlap(seg1, seg2):
    """
    辅助函数：计算 seg2 对 seg1 的重叠度 (Intersection / Length of seg1)
    seg1: 实际卡顿时段 (qoe)
    seg2: 预测卡顿时段 (calcu)
    """
    start1, end1 = seg1
    start2, end2 = seg2

    # 1. 计算交集
    overlap_start = max(start1, start2)
    overlap_end = min(end1, end2)
    intersection_len = max(0, overlap_end - overlap_start + 1)

    # 2. 计算分母 (实际卡顿时段的长度)
    seg1_len = end1 - start1 + 1

    if seg1_len == 0:
        return 0.0  # 理论上不发生

    # 3. 计算重叠度
    overlap_ratio = intersection_len / seg1_len
    return overlap_ratio

# 根据准确序列计算准确率和召回率的函数
def calculate_seq_metrics(seq_accuracy, qoe_data, calcu_data):
    """
    根据准确序列计算准确率和召回率
    seq_accuracies: 每个时段的准确序列
    """
    accuracy_1 = 0
    accuracy_0 = 0
    calcu_1 = 0
    calcu_0 = 0
    recall_1 = 0
    recall_0 = 0
    qoe_1 = 0
    qoe_0 = 0

    # 计算准确率
    # 计算准确率
    for i in range(len(seq_accuracy)):
        if calcu_data[i] == 1 :  # 评估结果为1时
            calcu_1 += 1
            if seq_accuracy[i] == 1:
                accuracy_1 += 1
        else:   # 评估结果为0时
            calcu_0 += 1
            if seq_accuracy[i] == 1:
                accuracy_0 += 1

    accuracy_1 = accuracy_1 / calcu_1
    accuracy_0 = accuracy_0 / calcu_0

    # 计算召回率
    for i in range(len(seq_accuracy)):
        if qoe_data[i] == 1:  # 实际结果为1时
            qoe_1 += 1
            if seq_accuracy[i] == 1:
                recall_1 += 1
        else:   # 实际结果为0时
            qoe_0 += 1
            if seq_accuracy[i] == 1:
                recall_0 += 1

    recall_1 = recall_1 / qoe_1
    recall_0 = recall_0 / qoe_0

    return accuracy_1, accuracy_0, recall_1, recall_0


# 使用方式
if __name__ == "__main__":
    # 载入数据文件
    calcu_data, qoe_data = np.loadtxt('gaming_result_comparison.txt', dtype=int)
    # 计算准确度和召回率
    accuracy, recall, seq_accuracy = calculate_metrics(qoe_data, calcu_data)
    accuracy_1, accuracy_0, recall_1, recall_0 = calculate_seq_metrics(seq_accuracy, qoe_data, calcu_data)

    print("Accuracy:", accuracy)
    print("Recall:", recall)
    print("Seq Accuracy:", seq_accuracy)
    print("Accuracy_1:", accuracy_1)
    print("Accuracy_0:", accuracy_0)
    print("Recall_1:", recall_1)
    print("Recall_0:", recall_0)

    # 使用滑动窗口容错法计算准确度
    accuracy_sw = sliding_window_tolerance(qoe_data, calcu_data, window_size=5, threshold=0.5)
    accuracy_sw_1, accuracy_sw_0, recall_sw_1, recall_sw_0 = calculate_seq_metrics(accuracy_sw, qoe_data, calcu_data)
    print("Sliding Window Accurate Sequent:", accuracy_sw)
    print("Sliding Window Accuracy 1:", accuracy_sw_1)
    print("Sliding Window Accuracy 0:", accuracy_sw_0)
    print("Sliding Window Recall 1:", recall_sw_1)
    print("Sliding Window Recall 0:", recall_sw_0)

    # 使用边缘模糊匹配法计算准确度
    accuracy_edge = edge_tolerance_matching(qoe_data, calcu_data, tolerance=3)
    accuracy_edge_1, accuracy_edge_0, recall_edge_1, recall_edge_0 = calculate_seq_metrics(accuracy_edge, qoe_data, calcu_data)
    print("Edge Tolerance Accurate Sequent:", accuracy_edge)
    print("Edge Tolerance Accuracy 1:", accuracy_edge_1)
    print("Edge Tolerance Accuracy 0:", accuracy_edge_0)
    print("Edge Tolerance Recall 1:", recall_edge_1)
    print("Edge Tolerance Recall 0:", recall_edge_0)

    # 使用卡顿时段匹配度计算准确度
    accuracy_segment = segment_matching(qoe_data, calcu_data, threshold=0.5)
    accuracy_segment_1, accuracy_segment_0, recall_segment_1, recall_segment_0 = calculate_seq_metrics(accuracy_segment, qoe_data, calcu_data)
    print("Segment Matching Accurate Sequent:", accuracy_segment)
    print("Segment Matching Accuracy 1:", accuracy_segment_1)
    print("Segment Matching Accuracy 0:", accuracy_segment_0)
    print("Segment Matching Recall 1:", recall_segment_1)
    print("Segment Matching Recall 0:", recall_segment_0)

    # 使用加权时段比对法计算准确度
    # accuracy_weighted = weighted_segment_matching(qoe_data, calcu_data, long_weight=2, short_weight=1)
    # print("Weighted Segment Matching Accuracy:", accuracy_weighted)
