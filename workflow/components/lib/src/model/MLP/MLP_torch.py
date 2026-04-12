import os
import sys
import csv
from contextlib import redirect_stdout
from pathlib import Path
import datetime
import glob
import numpy as np
import pandas as pd

from imblearn.over_sampling import SMOTE

from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

OP_TIME = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output_info")
os.makedirs(OUTPUT_DIR, exist_ok=True)

import secrets
import matplotlib.pyplot as plt

# 自己的包以及使用LSTM实验报告中的种子对应的划分
import src.data.select_tests as select_tests

def read_tests_column(csv_path: str, column_name: str = 'tests') -> list:
    """
    读取 CSV 文件中的指定列，该列每行为分号分隔的字符串，
    返回列表的列表，每个子列表对应一行的分割结果。

    :param csv_path: CSV 文件路径
    :param column_name: 列名，默认为 'tests'
    :return: 列表的列表，例如 [['gaming_2025102601', 'gaming_2025102502', ...], ...]
    """
    # 读取 CSV 文件
    df = pd.read_csv(csv_path)
    
    # 检查列是否存在
    if column_name not in df.columns:
        raise ValueError(f"CSV 文件中不存在列 '{column_name}'")
    
    # 对 tests 列进行分割
    result = []
    for val in df[column_name]:
        if pd.isna(val):          # 处理缺失值
            result.append([])
        else:
            # 按分号分割，并去除可能的前后空格
            items = [item.strip() for item in str(val).split(';')]
            result.append(items)
    return result

class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)

    def flush(self):
        for s in self.streams:
            s.flush()

class MLPClassifier(nn.Module):
    def __init__(self, input_dim, hidden_dims=[256, 128, 64], dropout=0.2):
        super(MLPClassifier, self).__init__()
        layers = []
        prev_dim = input_dim
        for h_dim in hidden_dims:
            layers.append(nn.Linear(prev_dim, h_dim))
            layers.append(nn.ReLU())
            layers.append(nn.Dropout(dropout))
            prev_dim = h_dim
        layers.append(nn.Linear(prev_dim, 1))  # 输出1个logit
        self.net = nn.Sequential(*layers)
    
    def forward(self, x):
        return self.net(x)

class MLPModel:
    def __init__(self, windowsize):
        self.model = None
        self.scaler = StandardScaler()
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        # 数据集
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.label_column = 'label'  # 标签列名
        # 标记
        self.history = None
        self.feature_columns = None
        self.is_scaler_fit = False  # 标记scaler是否已经fit过
        # 记录训练过程
        self.history = None
        self.train_losses = []
        self.val_losses = []

    def build_model(self, drop_out=0.2, learning_rate=1e-3):
            feature_num = len(self.feature_columns)
            self.model = MLPClassifier(input_dim=feature_num,
                                    dropout=drop_out)

            # 计算正样本权重：neg_num / pos_num
            y = self.y_train  # numpy 数组，值是 0/1
            pos_num = (y == 1).sum()
            neg_num = (y == 0).sum()
            if pos_num == 0:
                pos_weight_value = 1.0
            else:
                pos_weight_value = neg_num / max(pos_num, 1)

            pos_weight = torch.tensor([pos_weight_value], dtype=torch.float32).to(self.device)
            self.criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
            self.optimizer = optim.Adam(self.model.parameters(), lr=learning_rate)

    def process_file(self, file_path):
        """处理单个文件的数据"""
        # 加载CSV文件
        df = pd.read_csv(file_path)
        
        # 检查是否有标签列
        if self.label_column not in df.columns:
            print(f"错误: 文件{file_path}中不存在{self.label_column}列")
            raise ValueError(f"文件{file_path}中不存在{self.label_column}列")
        
        # 确定特征列
        if self.feature_columns is None:
            # 需要排除的.csv列 - 时间戳、label
            exclude_columns = [
                'timestamp', 'curTime_of_UTC8', 'curWindow',
                self.label_column]
            self.feature_columns = [col for col in df.columns if col not in exclude_columns]
            print(f"使用的特征列: {self.feature_columns}")
            print(f"特征列数量: {len(self.feature_columns)}")

        # 提取特征值和标签
        X = df[self.feature_columns].values
        y = df[self.label_column].values

        return X, y, df

    def load_files(self, input_data_dir, seed):
        if not os.path.exists(input_data_dir):
            raise ValueError(f"目录{input_data_dir}不存在")
        
        csv_files = [f for f in os.listdir(input_data_dir) if f.endswith('.csv')]
        if not csv_files:
            raise ValueError(f"目录{input_data_dir}中没有找到CSV文件")

        # 获取测试集文件名列表
        tests_list = select_tests.select_tests_from_dir(input_data_dir, seed=seed)
        print(f"使用种子：{seed}")
        print(f"使用测试集：{tests_list}")

        # 缓存所有文件数据，并收集训练集原始特征
        cached_data = []           # 存储 (is_test, X_raw, y, df)
        train_features_raw = []    # 用于拟合标准化器
        for csv_file in csv_files:
            file_path = os.path.join(input_data_dir, csv_file)
            is_test = csv_file in tests_list
            X_raw, y, df = self.process_file(file_path)
            cached_data.append((is_test, X_raw, y, df))
            if not is_test and X_raw.shape[0] > 0:
                train_features_raw.append(X_raw)

        if not train_features_raw:
            raise ValueError("没有有效的训练集数据，无法拟合标准化器")

        # 拟合标准化器
        train_features_raw = np.concatenate(train_features_raw, axis=0)
        scaler = StandardScaler()
        scaler.fit(train_features_raw)
        print("标准化器基于训练集拟合完成，特征维度：", train_features_raw.shape[1])

        # 第二次遍历：标准化并合并数据
        X_train_list, y_train_list = [], []
        X_test_list, y_test_list = [], []
        for is_test, X_raw, y, df in cached_data:
            if X_raw.shape[0] == 0:
                print(f"警告：文件 {df} 无有效数据，已跳过")
                continue
            X_scaled = scaler.transform(X_raw)
            if is_test:
                X_test_list.append(X_scaled)
                y_test_list.append(y)
            else:
                X_train_list.append(X_scaled)
                y_train_list.append(y)

        # 合并为数组
        self.X_train = np.concatenate(X_train_list, axis=0) if X_train_list else np.array([])
        self.y_train = np.concatenate(y_train_list, axis=0) if y_train_list else np.array([])
        self.X_test = np.concatenate(X_test_list, axis=0) if X_test_list else np.array([])
        self.y_test = np.concatenate(y_test_list, axis=0) if y_test_list else np.array([])

        print(f"训练集样本数: {self.X_train.shape[0]}, 测试集样本数: {self.X_test.shape[0]}")

    def train_model(self, batch_size=32, epochs=50, validation_split=0.2, verbose=1, use_smote=True):
        """
        训练模型
        :param batch_size: 批大小
        :param epochs: 训练轮数
        :param validation_split: 从训练集中分割出的验证集比例（0表示不使用验证集）
        :param verbose: 控制输出频率，1为每轮输出，0为静默
        :param use_smote: 是否对训练集使用 SMOTE 过采样
        """
        # 准备数据
        X = torch.tensor(self.X_train, dtype=torch.float32)
        y = torch.tensor(self.y_train, dtype=torch.float32)

        # 1. 分割训练集和验证集（如果需要）
        if validation_split > 0:
            n = len(X)
            n_val = int(n * validation_split)
            indices = torch.randperm(n)
            train_idx, val_idx = indices[n_val:], indices[:n_val]
            X_train_raw, y_train_raw = X[train_idx], y[train_idx]
            X_val, y_val = X[val_idx], y[val_idx]
        else:
            X_train_raw, y_train_raw = X, y
            X_val, y_val = None, None

        # 2. 可选：对训练集应用 SMOTE（仅当 use_smote=True）
        if use_smote:
            smote = SMOTE(random_state=42)
            X_train_np = X_train_raw.numpy()
            y_train_np = y_train_raw.numpy().ravel()
            X_resampled, y_resampled = smote.fit_resample(X_train_np, y_train_np)
            X_train = torch.tensor(X_resampled, dtype=torch.float32)
            y_train = torch.tensor(y_resampled, dtype=torch.float32)
        else:
            X_train, y_train = X_train_raw, y_train_raw

        # 3. 创建 DataLoader
        train_dataset = TensorDataset(X_train, y_train)
        train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
        if validation_split > 0:
            val_dataset = TensorDataset(X_val, y_val)
            val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
        else:
            val_loader = None

        # 移动模型到设备
        self.model.to(self.device)

        # 清空历史记录
        self.train_losses = []
        self.val_losses = []
        best_val_loss = float('inf')
        patience = 10  # 早停耐心值
        patience_counter = 0

        for epoch in range(epochs):
            # 训练阶段
            self.model.train()
            total_loss = 0
            total_correct = 0
            total_samples = 0
            for X_batch, y_batch in train_loader:
                X_batch, y_batch = X_batch.to(self.device), y_batch.to(self.device)
                self.optimizer.zero_grad()
                outputs = self.model(X_batch)
                loss = self.criterion(outputs.squeeze(1), y_batch)
                loss.backward()
                self.optimizer.step()
                total_loss += loss.item() * X_batch.size(0)
                preds = (torch.sigmoid(outputs) >= 0.5).float()
                total_correct += (preds == y_batch).sum().item()
                total_samples += y_batch.numel()

            avg_train_loss = total_loss / len(train_loader.dataset)
            train_acc = total_correct / total_samples
            self.train_losses.append(avg_train_loss)

            # 验证阶段
            avg_val_loss = 0
            val_acc = 0
            if val_loader is not None:
                self.model.eval()
                total_val_loss = 0
                val_correct = 0
                val_samples = 0
                with torch.no_grad():
                    for X_batch, y_batch in val_loader:
                        X_batch, y_batch = X_batch.to(self.device), y_batch.to(self.device)
                        outputs = self.model(X_batch)
                        loss = self.criterion(outputs.squeeze(1), y_batch)
                        total_val_loss += loss.item() * X_batch.size(0)
                        preds = (torch.sigmoid(outputs) >= 0.5).float()
                        val_correct += (preds == y_batch).sum().item()
                        val_samples += y_batch.numel()
                avg_val_loss = total_val_loss / len(val_loader.dataset)
                val_acc = val_correct / val_samples
                self.val_losses.append(avg_val_loss)

                # 早停逻辑
                if avg_val_loss < best_val_loss:
                    best_val_loss = avg_val_loss
                    patience_counter = 0
                else:
                    patience_counter += 1
                    if patience_counter >= patience:
                        print(f"Early stopping at epoch {epoch+1}")
                        break

            if verbose and (epoch+1) % 10 == 0:
                val_msg = f"val Loss: {avg_val_loss:.4f}, val_acc: {val_acc:.4f}" if val_loader else ""
                print(f"Epoch {epoch+1}/{epochs} - train Loss: {avg_train_loss:.4f}, train_acc: {train_acc:.4f} - {val_msg}")

        print("训练完成")

    def evaluate_model(self, seed, prob_threshold = 0.5, batch_size=32, output_dir=OUTPUT_DIR):
        """
        在测试集上评估模型，输出准确率、AUC、分类报告，并绘制损失曲线和混淆矩阵
        :param batch_size: 批大小
        :param plot: 是否绘制图形
        :param save_plot: 如果提供路径（字符串），则保存图像，否则显示
        """
        # 准备测试数据
        X_test = torch.tensor(self.X_test, dtype=torch.float32)
        y_test = torch.tensor(self.y_test, dtype=torch.float32)
        test_dataset = TensorDataset(X_test, y_test)
        test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

        self.model.eval()
        all_preds = []
        all_probs = []
        all_labels = []

        with torch.no_grad():
            for X_batch, y_batch in test_loader:
                X_batch = X_batch.to(self.device)
                outputs = self.model(X_batch)
                probs = torch.sigmoid(outputs).cpu().numpy()
                probs = probs.squeeze(1)
                preds = (probs >= prob_threshold).astype(int)
                all_probs.append(np.atleast_1d(probs))
                all_preds.append(np.atleast_1d(preds))
                all_labels.append(np.atleast_1d(y_batch.numpy()))

        # 计算指标
        y_true = np.concatenate(all_labels, axis=0).ravel()
        y_pred = np.concatenate(all_preds, axis=0).ravel()

        cm = confusion_matrix(y_true, y_pred)
        print(f"混淆矩阵：（used seed：{seed}）")
        print(cm)
        # 从混淆矩阵中取出 TN, FP, FN, TP
        tn, fp, fn, tp = cm.ravel()

        rate_0 = tn / (tn + fp) if (tn + fp) > 0 else 0.0 
        rate_1 = tp / (tp + fn) if (tp + fn) > 0 else 0.0  

        print(f"非卡顿判别: {tn}/({tn}+{fp}) = {rate_0:.4f}")
        print(f"卡顿判别: {tp}/({tp}+{fn}) = {rate_1:.4f}")

        # 创建左右两幅子图
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

        # 左图：训练 / 验证 loss 曲线
        if len(self.train_losses) > 0 and len(self.val_losses) > 0:
            epochs = range(1, len(self.train_losses) + 1)
            axes[0].plot(epochs, self.train_losses, label='Train Loss')
            axes[0].plot(epochs, self.val_losses, label='Val Loss')
            axes[0].set_xlabel('Epoch')
            axes[0].set_ylabel('Loss')
            axes[0].set_title('Train / Validation Loss')
            axes[0].legend()
        else:
            axes[0].text(0.5, 0.5, 'No loss history',
                         ha='center', va='center', fontsize=12)
            axes[0].axis('off')

        # 右图：混淆矩阵
        disp = ConfusionMatrixDisplay(confusion_matrix=cm,
                                      display_labels=['0', '1'])
        disp.plot(cmap=plt.cm.Blues, ax=axes[1], colorbar=False)
        axes[1].set_title("Confusion Matrix")

        plt.tight_layout()

        # 保存图像到 output_info
        fig_path = os.path.join(output_dir, f"cm_and_loss_{seed}_{OP_TIME}.png")
        fig.savefig(fig_path, dpi=300, bbox_inches="tight")
        print(f"混淆矩阵和loss曲线图已保存到: {fig_path}")

        # plt.show()

def main():
    # 预先配置
    cuda = torch.cuda.is_available()
    input_data_dir = r'E:\UDP2026\data\processed\input_nt_model\gaming'
    # 提取先前实验使用的种子
    exp_summary_log = r'E:\UDP2026\src\model\LSTM\output_info\gaming\summary_logs.csv'
    been_used_seed_list = []
    with open(exp_summary_log, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)          # 按列名读取
        for row in reader:
            been_used_seed_list.append(row['seed'])
    exp_tests_list = read_tests_column(exp_summary_log)

    # 为每个目标业务创建输出环境
    scene = Path(input_data_dir).name
    scene_output_dir = os.path.join(OUTPUT_DIR, scene)
    os.makedirs(scene_output_dir, exist_ok=True)

    # 超参
    used_epochs = 50
    used_bs = 32
    used_lr = 1e-3
    used_dropout = 0.2
    used_ws = 10
    used_patience = 15
    used_prob_threshold = 0.5

    # 关键指标
    r0, r1 = 0.0, 0.0
    cnt = 0

    for seed, used_tests in zip(been_used_seed_list, exp_tests_list):
        used_seed = seed
        for i in range(len(used_tests)):
            used_tests[i] += f'_combinedUDPModem_ws_{used_ws}.csv'
            
        log_path = os.path.join(scene_output_dir, f"log_{used_seed}_{OP_TIME}.txt")
        with open(log_path, "w", encoding="utf-8") as f:
            tee = Tee(sys.stdout, f)
            with redirect_stdout(tee):
                print(f"日志保存到: {log_path}")

                if cuda == True:
                    print("使用设备：CUDA")
                else:
                    print("使用设备：CPU")
                print(
                    f"使用超参 | "
                    f"epochs={used_epochs}, bs={used_bs}, lr={used_lr}, "
                    f"dropout={used_dropout}, sq_len={used_ws}, "
                    f"patience={used_patience}, prob_threshold={used_prob_threshold}"
                )
                MLP_model = MLPModel(windowsize=used_ws)
                MLP_model.load_files(input_data_dir, seed=used_seed, tests=used_tests)

                MLP_model.build_model(drop_out=used_dropout, learning_rate=used_lr)
                MLP_model.train_model(epochs=used_epochs, batch_size=used_bs)
                MLP_model.evaluate_model(seed=used_seed, output_dir=scene_output_dir)
        
        print(f"本次运行日志已保存到: {log_path}")

if __name__ == '__main__':
    main()