import os
import sys
import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
# 将工程目录添加至python环境中
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
component_dir = os.path.join(project_root, "components")
sys.path.append(component_dir)
# 包含必要的库
# 这里将会包含可视化中必要的内容


class vis_in_modelEval:
    __name = "visualier_in_modelEval"

    def __init__(self):
        pass

    """
    @brief  模型训练完后的可视化
    @param  test_info   单一测试集文件与模型测试结果
    @param  opt_path    可视化结果的持久化路径
    @param  is_export   是否导出可视化结果
    @return result  可视化结果的状态码，0为成功，其他为失败
    @note   test_info  中包含了如下信息：
        - file_path : str                       测试集特征文件的原始路径
        - timestamp/curTime_of_UTC8 : list     测试集的时间序列
        - label : list                          测试集的标签(人工标准答案)
        - test_length : int                     测试集区间长度
        - y_prob : list[float]                  测试结果的回归概率序列
        - y_pred : list[int]                    测试结果的二分类序列
    @note   visual函数每次被执行时，所接收的数据，是一个测试集文件的测试结果、特征文件路径、标签信息、时间戳信息
    """
    def visual(self, test_info: pd.DataFrame, opt_path: str, is_export: bool = False) -> int:
        result = 0
        
        print("-"*60)
        print(f"### {self.__name} - visual Info: Start to Visual test results.")

        try:
            # 1. 提取test_info中的数据
            file_path = test_info['file_path'].iloc[0] if isinstance(test_info['file_path'], pd.Series) else test_info['file_path']
            timestamps = test_info['timestamp'].tolist() if 'timestamp' in test_info.columns else test_info['curTime_of_UTC8'].tolist()
            labels = test_info['label'].tolist()
            y_prob = test_info['probability'].tolist()
            y_pred = test_info['pred_label'].tolist()

            # 2. 读取原始特征文件
            feature_df = pd.read_csv(file_path)
            
            # 3. 确定时间列
            time_col = 'timestamp' if 'timestamp' in feature_df.columns else 'curTime_of_UTC8'
            feature_timestamps = feature_df[time_col].tolist()

            # 4. 确定特征列（排除label, curTime_of_UTC8, timestamp）
            exclude_cols = ['label', 'curTime_of_UTC8', 'timestamp']
            feature_cols = [col for col in feature_df.columns if col not in exclude_cols]

            # 5. 创建导出目录（如果需要导出）
            if is_export:
                import os
                dir_name = os.path.basename(file_path).split('.')[0]
                export_dir = os.path.join(opt_path, dir_name)
                os.makedirs(export_dir, exist_ok=True)

            # 6. 为每个特征绘制图表
            for feature_name in feature_cols:
                fig, ax1 = plt.subplots(figsize=(12, 6))

                # 绘制特征曲线（左侧Y轴）
                feature_data = feature_df[feature_name].tolist()
                ax1.plot(feature_timestamps, feature_data, color='green', label=f'{feature_name}')
                ax1.set_xlabel('Timestamp')
                ax1.set_ylabel(feature_name)
                ax1.tick_params(axis='y')
                
                # 优化x轴刻度显示，使用MaxNLocator确保无论如何拉伸图像，横坐标始终保持10个均匀分布的刻度
                ax1.xaxis.set_major_locator(MaxNLocator(nbins=10))
                plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

                # 创建右侧Y轴用于概率曲线
                ax2 = ax1.twinx()
                ax2.plot(timestamps, y_prob, color='orange', label='Prediction Probability')
                ax2.set_ylabel('Probability (0-1)')
                ax2.set_ylim(0, 1)
                ax2.tick_params(axis='y')

                # 绘制背景颜色区间
                # 找出人工标签为1的区间（红色）
                label_intervals = []
                start = None
                for i, val in enumerate(labels):
                    if val == 1 and start is None:
                        start = timestamps[i]
                    elif val != 1 and start is not None:
                        label_intervals.append((start, timestamps[i-1]))
                        start = None
                if start is not None:
                    label_intervals.append((start, timestamps[-1]))

                # 找出模型预测为1的区间（蓝色）
                pred_intervals = []
                start = None
                for i, val in enumerate(y_pred):
                    if val == 1 and start is None:
                        start = timestamps[i]
                    elif val != 1 and start is not None:
                        pred_intervals.append((start, timestamps[i-1]))
                        start = None
                if start is not None:
                    pred_intervals.append((start, timestamps[-1]))

                # 找出重叠区间（紫色）
                overlap_intervals = []
                for l_start, l_end in label_intervals:
                    for p_start, p_end in pred_intervals:
                        overlap_start = max(l_start, p_start)
                        overlap_end = min(l_end, p_end)
                        if overlap_start < overlap_end:
                            overlap_intervals.append((overlap_start, overlap_end))

                # 绘制重叠区间（紫色，需要先绘制以避免被覆盖）
                for start, end in overlap_intervals:
                    ax1.axvspan(start, end, alpha=0.3, color='purple', label='Overlap (Label & Pred)')

                # 绘制标签区间（红色）
                for start, end in label_intervals:
                    is_overlap = False
                    for o_start, o_end in overlap_intervals:
                        if start >= o_start and end <= o_end:
                            is_overlap = True
                            break
                    if not is_overlap:
                        ax1.axvspan(start, end, alpha=0.3, color='red', label='Manual Label')

                # 绘制预测区间（蓝色）
                for start, end in pred_intervals:
                    is_overlap = False
                    for o_start, o_end in overlap_intervals:
                        if start >= o_start and end <= o_end:
                            is_overlap = True
                            break
                    if not is_overlap:
                        ax1.axvspan(start, end, alpha=0.3, color='blue', label='Model Prediction')

                # 添加图例
                handles1, labels1 = ax1.get_legend_handles_labels()
                handles2, labels2 = ax2.get_legend_handles_labels()
                
                # 去重图例
                all_handles = handles1 + handles2
                all_labels = labels1 + labels2
                unique_handles = []
                unique_labels = []
                for h, l in zip(all_handles, all_labels):
                    if l not in unique_labels:
                        unique_labels.append(l)
                        unique_handles.append(h)
                
                ax1.legend(unique_handles, unique_labels, loc='upper left')

                plt.title(f'{feature_name} - Feature Analysis')
                plt.tight_layout()

                # 根据is_export决定显示或保存
                if is_export:
                    plt.savefig(os.path.join(export_dir, f'{feature_name}.png'), dpi=100)
                    plt.close(fig)
                else:
                    plt.show()

            print(f"### {self.__name} - visual Info: Visualization completed successfully.")
            print("-"*60)

        except Exception as e:
            print(f"### {self.__name} - visual Error: {str(e)}")
            print("-"*60)
            result = -1

        return result


