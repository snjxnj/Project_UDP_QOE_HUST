import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
import os
import re

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "LSTM")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

class LSTMModel:
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.history = None
        self.sequence_length = 20  # 时间序列长度
        self.feature_columns = None
        self.label_column = 'label'  # 标签列名
        self.is_scaler_fit = False  # 标记scaler是否已经fit过
    
    def load_data(self, file_path):
        """加载数据集并进行预处理"""
        # 加载CSV文件
        df = pd.read_csv(file_path)
        
        # 显示数据基本信息
        print(f"数据集形状: {df.shape}")
        print(f"数据集前5行:\n{df.head()}")
        
        # 使用label作为标签，排除前两列curTime_of_UTC8和curWindow，使用其余列作为特征值
        if self.label_column in df.columns:
            # 排除前两列和标签列，使用其余列作为特征列
            exclude_columns = [
                'curTime_of_UTC8', 'curWindow',
                'BW',
                'RSSI',
                'RSRP',
                'RSRQ',
                'SINR10',
                'uR_mean',
                'dR_mean',
                'uM_mean',
                'dM_mean',
                'uG_mean',
                'dG_mean',
                'uB_mean',
                'dB_mean',
                'txp_mean',
                'mtpl_mean',
                'cqi_mean',
                'mac_mean',
                'pps_mean',
                'ppr_mean',
                'ssnr_mean',
                'pathLoss_mean',
                'targetPWR_mean',
                'rsrp4_mean',
                'snr4_mean',
                self.label_column]
            self.feature_columns = [col for col in df.columns if col not in exclude_columns]
            print(f"特征列数量: {len(self.feature_columns)}")
            
            # 提取特征值和标签
            X = df[self.feature_columns].values
            y = df[self.label_column].values
            
            print(f"使用的特征列: {self.feature_columns}")
            print(f"特征列数量: {len(self.feature_columns)}")
        else:
            # 如果没有label列，打印错误信息
            print(f"错误: 数据集中不存在{self.label_column}列")
            raise ValueError(f"数据集中不存在{self.label_column}列")
        
        # 数据标准化
        if not self.is_scaler_fit:
            X_scaled = self.scaler.fit_transform(X)
            self.is_scaler_fit = True
        else:
            X_scaled = self.scaler.transform(X)
        
        # 创建时间序列数据
        X_sequences, y_sequences = self.create_sequences(X_scaled, y)
        
        # 划分训练集和测试集
        # 按时间顺序分割
        train_size = int(len(X_sequences) * 0.8)
        self.X_train, self.X_test = X_sequences[:train_size], X_sequences[train_size:]
        self.y_train, self.y_test = y_sequences[:train_size], y_sequences[train_size:]
        
        print(f"训练集形状: X={self.X_train.shape}, y={self.y_train.shape}")
        print(f"测试集形状: X={self.X_test.shape}, y={self.y_test.shape}")
        
        return X_sequences, y_sequences
    
    def load_multiple_files(self, directory_path):
        """加载多个文件，根据文件名前4位数字区分训练集(偶数)和测试集(奇数)"""
        # 检查目录是否存在
        if not os.path.exists(directory_path):
            print(f"错误: 目录{directory_path}不存在")
            raise ValueError(f"目录{directory_path}不存在")
        
        # 获取目录中所有CSV文件
        csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
        
        if not csv_files:
            print(f"错误: 目录{directory_path}中没有找到CSV文件")
            raise ValueError(f"目录{directory_path}中没有找到CSV文件")
        
        # 初始化训练集和测试集
        train_sequences = []
        train_labels = []
        test_sequences = []
        test_labels = []
        
        # 遍历所有CSV文件
        for csv_file in csv_files:
            # 尝试提取文件名前4位数字
            match = re.match(r'(\d{4})', csv_file)
            if match:
                prefix = match.group(1)
                try:
                    prefix_num = int(prefix)
                    # print(f"head_number: {prefix_num}")
                    file_path = os.path.join(directory_path, csv_file)
                    print(f"处理文件: {csv_file}, 前缀: {prefix}, 类型: {'训练集' if prefix_num % 2 == 0 else '测试集'}")
                    
                    # 加载单个文件的数据
                    X_sequences, y_sequences = self._load_single_file_for_multiple(file_path)
                    
                    # 根据前缀数字的奇偶性添加到对应的集合
                    if prefix_num % 2 == 0:  # 偶数是训练集
                        train_sequences.append(X_sequences)
                        train_labels.append(y_sequences)
                    else:  # 奇数是测试集
                        test_sequences.append(X_sequences)
                        test_labels.append(y_sequences)
                except ValueError:
                    print(f"警告: 在遍历过程中，文件{csv_file}的前4位不是有效数字，跳过该文件")
            else:
                print(f"警告: 由于匹配前4位数字失败，文件{csv_file}不以4位数字开头，跳过该文件")
        
        # 合并所有训练集和测试集
        if train_sequences:
            self.X_train = np.concatenate(train_sequences, axis=0)
            self.y_train = np.concatenate(train_labels, axis=0)
        else:
            print("警告: 没有找到训练集文件")
            self.X_train = np.array([])
            self.y_train = np.array([])
        
        if test_sequences:
            self.X_test = np.concatenate(test_sequences, axis=0)
            self.y_test = np.concatenate(test_labels, axis=0)
        else:
            print("警告: 没有找到测试集文件")
            self.X_test = np.array([])
            self.y_test = np.array([])
        
        print(f"总训练集形状: X={self.X_train.shape}, y={self.y_train.shape}")
        print(f"总测试集形状: X={self.X_test.shape}, y={self.y_test.shape}")
        
        # 确保训练集不为空
        if len(self.X_train) == 0:
            raise ValueError("没有有效的训练数据，请检查文件命名是否正确")
            
    def _load_single_file_for_multiple(self, file_path):
        """为多文件模式加载单个文件的数据"""
        # 加载CSV文件
        df = pd.read_csv(file_path)
        
        # 检查是否有标签列
        if self.label_column not in df.columns:
            print(f"错误: 文件{file_path}中不存在{self.label_column}列")
            raise ValueError(f"文件{file_path}中不存在{self.label_column}列")
        
        # 如果是第一次处理文件，确定特征列
        if self.feature_columns is None:
            exclude_columns = [
                'timestamp', 'curTime_of_UTC8', 'curWindow',

                # 'num_send_packets','num_recv_packets',
                # 'avg_send_packetLen','avg_recv_packetLen',
                # 'send_dataStream','recv_dataStream',
                # 'avg_send_packetInterval','avg_recv_packetInterval',
                # 'max_send_packetInterval','max_recv_packetInterval',
                # 'min_send_packetInterval','min_recv_packetInterval',
                # 'std_send_packetInterval','std_recv_packetInterval',
                # 'cv_send_packetInterval','cv_recv_packetInterval',
                # 'send_Mutation_of_numPackets','recv_Mutation_of_numPackets'
                
                # 'mn_u_r_mean','mn_d_r_mean',
                # 'mn_u_m_mean','mn_d_m_mean',
                # 'mn_d_g_mean','mn_u_g_mean',
                # 'mn_u_b_mean','mn_d_b_mean',
                'mn_txp_md_mean',
                # 'mn_cqi_mean',
                # 'mn_mac_mean',
                'mn_pu_m_l_mean','mn_pd_m_l_mean',
                'mn_pp_r_mean',
                # 'mn_ssnr_mean',
                'mn_tsnr_mean',
                'mn_path_loss_mean',
                'mn_target_pwr_mean',
                'mn_bandwidth_mean',
                # 'rsrp_1_mean','rsrp_2_mean',
                'rsrp_3_mean','rsrp_4_mean',
                # 'snr_1_mean','snr_2_mean',
                'snr_3_mean','snr_4_mean',

                'BW',
                'RSSI',
                'RSRP',
                'RSRQ',
                'SINR10',
                'uR_mean','dR_mean',
                'uM_mean','dM_mean',
                'uG_mean','dG_mean',
                'uB_mean','dB_mean',
                'txp_mean','mtpl_mean',
                'cqi_mean',
                'mac_mean',
                'pps_mean','ppr_mean',
                'ssnr_mean',
                'pathLoss_mean',
                'targetPWR_mean',
                'rsrp4_mean','snr4_mean',
                self.label_column]
            self.feature_columns = [col for col in df.columns if col not in exclude_columns]
            print(f"使用的特征列: {self.feature_columns}")
            print(f"特征列数量: {len(self.feature_columns)}")

        # 提取特征值和标签
        X = df[self.feature_columns].values
        y = df[self.label_column].values

        # 数据标准化
        if not self.is_scaler_fit:
            X_scaled = self.scaler.fit_transform(X)
            self.is_scaler_fit = True
        else:
            X_scaled = self.scaler.transform(X)
        
        # 创建时间序列数据 - 只在单个文件内创建序列，解决时间不连续问题
        X_sequences, y_sequences = self.create_sequences(X_scaled, y)
        return X_sequences, y_sequences
    
    def create_sequences(self, X, y):
        """将数据转换为LSTM需要的序列格式"""
        X_sequences = []
        y_sequences = []
        
        for i in range(len(X) - self.sequence_length):
            X_sequences.append(X[i:i + self.sequence_length])
            y_sequences.append(y[i + self.sequence_length])
        
        return np.array(X_sequences), np.array(y_sequences)
    
    def build_model(self):
        """构建LSTM模型"""
        self.model = Sequential()
        
        # 添加LSTM层
        self.model.add(LSTM(128, return_sequences=True, input_shape=(self.sequence_length, len(self.feature_columns))))
        self.model.add(Dropout(0.2))

        self.model.add(LSTM(64, return_sequences=True))
        self.model.add(Dropout(0.2))
        
        self.model.add(LSTM(32))
        self.model.add(Dropout(0.2))
        
        # 添加全连接层
        self.model.add(Dense(32, activation='relu'))
        self.model.add(Dropout(0.2))
        
        self.model.add(Dense(16, activation='relu'))
        
        # 输出层（二分类问题使用sigmoid，多分类使用softmax）
        self.model.add(Dense(1, activation='sigmoid'))
        
        # 编译模型
        self.model.compile(
            optimizer='adam',
            loss='binary_crossentropy',
            metrics=['accuracy']
        )
        
        # 打印模型结构
        self.model.summary()
    
    def train_model(self, epochs=50, batch_size=32):
        """训练模型"""
        # 设置早停策略
        early_stopping = EarlyStopping(
            monitor='val_loss',
            patience=10,
            restore_best_weights=True
        )
        
        # 训练模型
        self.history = self.model.fit(
            self.X_train,
            self.y_train,
            epochs=epochs,
            batch_size=batch_size,
            validation_data=(self.X_test, self.y_test),
            callbacks=[early_stopping],
            verbose=1
        )
        
        # 绘制训练历史
        self.plot_training_history()
    
    def plot_training_history(self):
        """绘制训练和验证的损失和准确率曲线"""
        if self.history is None:
            print("模型尚未训练，无法绘制训练历史")
            return
        
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        
        # 绘制损失曲线
        axes[0].plot(self.history.history['loss'], label='训练损失')
        axes[0].plot(self.history.history['val_loss'], label='验证损失')
        axes[0].set_title('训练和验证损失')
        axes[0].set_xlabel('轮次')
        axes[0].set_ylabel('损失')
        axes[0].legend()
        
        # 绘制准确率曲线
        axes[1].plot(self.history.history['accuracy'], label='训练准确率')
        axes[1].plot(self.history.history['val_accuracy'], label='验证准确率')
        axes[1].set_title('训练和验证准确率')
        axes[1].set_xlabel('轮次')
        axes[1].set_ylabel('准确率')
        axes[1].legend()
        
        plt.tight_layout()
        plt.show()
    
    def evaluate_model(self):
        """评估模型性能"""
        if self.model is None:
            print("模型尚未构建，无法评估")
            return
        
        loss, accuracy = self.model.evaluate(self.X_test, self.y_test, verbose=0)
        print(f"测试集损失: {loss:.4f}")
        print(f"测试集准确率: {accuracy:.4f}")
        
        # 获取预测结果
        y_pred = self.model.predict(self.X_test)
        y_pred_classes = (y_pred > 0.5).astype(int)
        
        # 显示预测结果示例
        print("\n预测结果示例:")
        print(f"真实标签: {self.y_test[:5].ravel()}")
        print(f"预测概率: {y_pred[:5].ravel()}")
        print(f"预测类别: {y_pred_classes[:5].ravel()}")
        
        # 生成并显示混淆矩阵
        print("\n混淆矩阵:")
        cm = confusion_matrix(self.y_test, y_pred_classes)
        
        # 打印混淆矩阵数值
        print(cm)
        
        # 可视化混淆矩阵
        plt.figure(figsize=(8, 6))
        disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=['类别0', '类别1'])
        disp.plot(cmap=plt.cm.Blues)
        plt.title('混淆矩阵')
        plt.savefig(os.path.join(OUTPUT_DIR, 'confusion_matrix.png'))
        plt.show()
        
        # 保存预测结果和真实标签到txt文件
        print("\n正在保存预测结果和真实标签到result_lstm.txt文件...")
        with open(os.path.join(OUTPUT_DIR, 'result_lstm.txt'), 'w') as f:
            # 保存模型预测结果（一行）
            pred_str = ' '.join(map(str, y_pred_classes.ravel()))
            f.write(pred_str + '\n')
            # 保存验证集原始标签（一行）
            true_str = ' '.join(map(str, self.y_test.ravel()))
            f.write(true_str + '\n')
        print("预测结果和真实标签已保存到result_lstm.txt文件")
    
    def save_model(self, model_path='lstm_model.h5'):
        """保存模型"""
        if self.model is None:
            print("模型尚未构建，无法保存")
            return
        
        full_path = os.path.join(OUTPUT_DIR, model_path)
        self.model.save(full_path)
        print(f"模型已保存到: {model_path}")

# 主函数
def main():
    # 创建并训练LSTM模型
    lstm_model = LSTMModel()
    
    # 加载多个文件数据（可以选择下面两种方式之一）
    
    # 方式1: 加载单个文件（原有功能）
    # data_file = 'extracted_UDP_features_labeled_cleaned.csv'
    # print("正在加载数据...")
    # lstm_model.load_data(data_file)
    
    # 方式2: 加载多个文件（新功能）
    print("正在加载多个文件数据...")
    # 指定包含CSV文件的目录路径
    directory_path = '.'  # 当前目录
    lstm_model.load_multiple_files(directory_path)
    
    # 构建模型
    print("\n正在构建模型...")
    lstm_model.build_model()
    
    # 训练模型
    print("\n正在训练模型...")
    lstm_model.train_model(epochs=100, batch_size=32)  # 调整为合理的epochs数量
    
    # 评估模型
    print("\n正在评估模型...")
    lstm_model.evaluate_model()
    
    # 保存模型
    print("\n正在保存模型...")
    lstm_model.save_model()

if __name__ == "__main__":
    main()