import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay, precision_recall_fscore_support
from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout, Bidirectional, GlobalAveragePooling1D
from keras.callbacks import EarlyStopping
# 导入TCNBlock类
from TCNBlock import TCN
import tensorflow as tf
import logging
import os
import re
import seaborn as sns
import shap

# 设置中文显示
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

def setup_logging():
    '''设置日志配置'''
    # 确保logs目录存在
    if not os.path.exists('logs'):
        os.makedirs('logs')
    logging.basicConfig(
        # 在文件名上添加实时时间戳，避免覆盖
        filename= 'logs/tcn_model_{}_log.txt'.format(time.strftime('%Y%m%d_%H%M%S')),
        level=logging.INFO,       # 设置日志级别为 INFO，记录所有信息
        format='%(asctime)s - %(levelname)s - %(message)s',  # 日志格式
        datefmt='%Y-%m-%d %H:%M:%S',  # 日期时间格式
        encoding = 'utf-8'  # 设置日志文件的编码为 UTF-8
    )

class TCNModel:
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
                # 'BW',
                'RSSI',
                'RSRP',
                'RSRQ',
                'SINR10',
                'uR_mean',
                'dR_mean',
                # 'uM_mean',
                # 'dM_mean',
                # 'uG_mean',
                # 'dG_mean',
                'uB_mean',
                'dB_mean',
                'txp_mean',
                'mtpl_mean',
                'cqi_mean',
                # 'mac_mean',
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
                    print(f"警告: 文件{csv_file}的前4位不是有效数字，跳过该文件")
            else:
                print(f"警告: 文件{csv_file}不以4位数字开头，跳过该文件")
        
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

        # 检查标签分布
        print("训练集标签分布:")
        unique, counts = np.unique(self.y_train, return_counts=True)
        for u, c in zip(unique, counts):
            print(f"类别 {u}: {c} ({c/len(self.y_train)*100:.2f}%)")

        print("测试集标签分布:")
        unique, counts = np.unique(self.y_test, return_counts=True)
        for u, c in zip(unique, counts):
            print(f"类别 {u}: {c} ({c/len(self.y_test)*100:.2f}%)")
        
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
                'curTime_of_UTC8', 'curWindow',
                # 'std_send_packetInterval',
                # 'std_recv_packetInterval',
                # 'BW',
                'RSSI',
                'RSRP',
                'RSRQ',
                'SINR10',
                # 'uR_mean','dR_mean',
                'uM_mean','dM_mean',
                'uG_mean','dG_mean',
                # 'uB_mean','dB_mean',
                'txp_mean','mtpl_mean',
                'cqi_mean',
                # 'mac_mean',
                'pps_mean','ppr_mean',
                # 'ssnr_mean',
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
        """构建TCN模型"""
        input_shape = (self.sequence_length, len(self.feature_columns)) # (时间步长, 特征数量)

        input_layer = tf.keras.Input(shape=input_shape) # 输入层

        x = TCN(
            filters=64, 
            kernel_size=3, 
            dilations=[1, 2, 4], 
            dropout_rate=0.2)(input_layer) # TCN层

        x = tf.keras.layers.GlobalAveragePooling1D()(x) # 全局平均池化层

        x = tf.keras.layers.Dense(64, activation='relu')(x) # 全连接层

        x = tf.keras.layers.Dropout(0.2)(x) # Dropout层

        output_layer = tf.keras.layers.Dense(1, activation='sigmoid')(x) # 输出层

        self.model = tf.keras.Model(inputs=input_layer, outputs=output_layer)

        # 编译模型
        self.model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=0.0005),
            loss='binary_crossentropy', 
            metrics=[
                'accuracy',
                tf.keras.metrics.AUC(name='auc')
            ]
        )

        # 模型构建
        self.model.build((None, self.sequence_length, len(self.feature_columns)))

        # 输出模型结构
        self.model.summary()        
        
    
    def train_model(self, epochs=50, batch_size=32):
        """训练模型"""
        # 设置日志记录
        setup_logging()

        # 计算类别权重
        from sklearn.utils import class_weight
        y_train_int = self.y_train.astype(int)
        class_weights = class_weight.compute_class_weight(
            'balanced',
            classes=np.unique(y_train_int),
            y=y_train_int
        )
        class_weight_dict = dict(enumerate(class_weights))
        print(f"类别权重: {class_weight_dict}")
        logging.info(f"类别权重: {class_weight_dict}")

        # 设置早停策略
        early_stopping = EarlyStopping(
            monitor='val_loss',
            patience=10,
            restore_best_weights=True
        )

        # 记录开始训练的日志
        logging.info(f"开始训练模型，epochs={epochs}, batch_size={batch_size}")
        
        # 训练模型
        self.history = self.model.fit(
            self.X_train,
            self.y_train,
            epochs=epochs,
            batch_size=batch_size,
            validation_data=(self.X_test, self.y_test),
            # callbacks=[early_stopping],
            class_weight=class_weight_dict,
            verbose=1
        )

        # 记录每轮训练的日志
        num_epochs = len(self.history.history['loss'])  # 获取实际训练的轮次
        for epoch in range(num_epochs):
            logging.info(f"Epoch {epoch + 1}/{num_epochs} - "
                         f"loss: {self.history.history['loss'][epoch]:.4f} - "
                         f"accuracy: {self.history.history['accuracy'][epoch]:.4f} - "
                         f"val_loss: {self.history.history['val_loss'][epoch]:.4f} - "
                         f"val_accuracy: {self.history.history['val_accuracy'][epoch]:.4f}")
        
        # 绘制训练历史
        self.plot_training_history()
        logging.info("训练完成")
    
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

    def calculate_permutation_importance(self):
        """排列重要性"""
        # 注意：LSTM输入是3D (samples, sequence_length, features)，需适配排列逻辑
        # 方法：对测试集的每个特征，在所有时间步上打乱，计算准确率下降幅度（降幅越高，特征越重要）
        baseline_accuracy = self.model.evaluate(self.X_test, self.y_test, verbose=0)[1]
        importances = []

        for feat_idx in range(len(self.feature_columns)):
            # 复制测试集，避免修改原数据
            X_test_permuted = self.X_test.copy()
            # 打乱该特征在所有样本、所有时间步的值
            for seq_idx in range(self.sequence_length):
                np.random.shuffle(X_test_permuted[:, seq_idx, feat_idx])
            # 计算打乱后的准确率
            permuted_accuracy = self.model.evaluate(X_test_permuted, self.y_test, verbose=0)[1]
            # 重要性 = 基准准确率 - 打乱后准确率（降幅越大，特征越重要）
            importance = baseline_accuracy - permuted_accuracy
            importances.append(importance)

        # 整理结果
        imp_df = pd.DataFrame({
            'feature': self.feature_columns,
            'permutation_importance': importances
        }).sort_values('permutation_importance', ascending=False)

        print("=== 排列重要性（Top10）===")
        print(imp_df.head(10))

        # 可视化
        plt.figure(figsize=(10, 8))
        sns.barplot(x='permutation_importance', y='feature', data=imp_df.head(10))
        plt.title('LSTM特征排列重要性（Top10）')
        plt.xlabel('准确率降幅')
        plt.ylabel('特征')
        plt.show()

        return imp_df

    def evaluate_model(self):
        """评估模型性能"""
        if self.model is None:
            print("模型尚未构建，无法评估")
            return

        results = self.model.evaluate(self.X_test, self.y_test, verbose=0)
        loss = results[0]
        accuracy = results[1]
        auc_score = results[2] if len(results) > 2 else None

        print(f"测试集损失: {loss:.4f}")
        print(f"测试集准确率: {accuracy:.4f}")
        if auc_score is not None:
            print(f"测试集AUC: {auc_score:.4f}")
        logging.info(f"测试集损失: {loss:.4f}")
        logging.info(f"测试集准确率: {accuracy:.4f}")
        if auc_score is not None:
            logging.info(f"测试集AUC: {auc_score:.4f}")

        # 获取预测结果
        y_pred = self.model.predict(self.X_test)

        # 寻找最佳阈值
        from sklearn.metrics import roc_curve, auc
        fpr, tpr, thresholds = roc_curve(self.y_test, y_pred)
        roc_auc = auc(fpr, tpr)

        # 选择最佳阈值（Youden's J statistic）
        youden_j = tpr - fpr
        best_threshold = thresholds[np.argmax(youden_j)]
        print(f"最佳阈值: {best_threshold:.4f}")
        print(f"ROC AUC: {roc_auc:.4f}")

        # 使用最佳阈值进行预测
        y_pred_classes = (y_pred > best_threshold).astype(int)

        # 显示预测结果示例
        print("\n预测结果示例:")
        print(f"真实标签: {self.y_test[:5].ravel()}")
        print(f"预测概率: {y_pred[:5].ravel()}")
        print(f"预测类别: {y_pred_classes[:5].ravel()}")
        logging.info(
            f"预测结果示例: \n真实标签: {self.y_test[:5].ravel()}, \n预测概率: {y_pred[:5].ravel()}, \n预测类别: {y_pred_classes[:5].ravel()}")

        # 生成并显示混淆矩阵
        print("\n混淆矩阵:")
        cm = confusion_matrix(self.y_test, y_pred_classes)

        # 计算精确率、召回率和F1分数
        precision, recall, f1, _ = precision_recall_fscore_support(self.y_test, y_pred_classes, average='binary')

        # 打印混淆矩阵数值
        print(cm)
        print(f"精确率: {precision:.4f}")
        print(f"召回率: {recall:.4f}")
        print(f"F1分数: {f1:.4f}")
        logging.info(f"混淆矩阵: \n{cm}")
        logging.info(f"精确率: {precision:.4f}")
        logging.info(f"召回率: {recall:.4f}")
        logging.info(f"F1分数: {f1:.4f}")

        # 可视化混淆矩阵
        plt.figure(figsize=(8, 6))
        disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=['类别0', '类别1'])
        disp.plot(cmap=plt.cm.Blues)
        plt.title('混淆矩阵')
        plt.show()

    def save_model(self, model_path='tcn_model.h5'):
        """保存模型"""
        # 将模型保存到trained_models文件夹，添加时间戳以避免覆盖
        if not os.path.exists('trained_models'):
            os.makedirs('trained_models')

        model_path = os.path.join('trained_models', f'tcn_{int(time.time())}.h5')
        # 检测模型是否存在
        if self.model is None:
            print("模型尚未构建，无法保存")
            return

        self.model.save(model_path)
        print(f"模型已保存到: {model_path}")

# 主函数
def main():
    # 创建并训练TCN模型
    tcn_model = TCNModel()
    
    # 加载多个文件
    print("正在加载多个文件数据...")
    # 指定包含CSV文件的目录路径
    directory_path = '.'  # 当前目录
    tcn_model.load_multiple_files(directory_path)
    
    # 构建模型
    print("\n正在构建模型...")
    tcn_model.build_model()
    
    # 训练模型
    print("\n正在训练模型...")
    tcn_model.train_model(epochs=50, batch_size=32)  # 调整为合理的epochs数量

    # 计算排列重要性
    print("\n正在计算排列重要性...")
    tcn_model.calculate_permutation_importance()
    
    # 评估模型
    print("\n正在评估模型...")
    tcn_model.evaluate_model()
    
    # 保存模型
    # print("\n正在保存模型...")
    # tcn_model.save_model()

if __name__ == "__main__":
    main()