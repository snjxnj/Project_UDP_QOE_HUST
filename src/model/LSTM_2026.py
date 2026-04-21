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

# 加载本地包
import src.data_loader.load_files as load_files

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
		# 模型数据集
		self.X_train = None
		self.X_val = None
		self.X_test = None
		self.y_train = None
		self.y_val = None
		self.y_test = None
		self.test_info = None
		
		self.history = None
		self.sequence_length = 20  # 时间序列长度
		self.feature_columns = None
		self.label_column = 'label'  # 标签列名

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
			validation_data=(self.X_val, self.y_val),
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

	# 新增的两个方法 - 切分出测试集的完整信息
	def build_dataframe_from_test_info(self, test_dict):
		rows = []

		file_name = test_dict['file_name']
		timestamps = test_dict.get('curTime_of_UTC8', [None] * test_dict['test_length'])
		true_labels = test_dict['label']
		probs = test_dict['probability']
		pred_labels = test_dict['pred_label']
		
		# 确保长度一致
		n = test_dict['test_length']
		for i in range(n):
			rows.append({
				'file_name': file_name,
				'curTime_of_UTC8': timestamps[i] if timestamps and i < len(timestamps) else None,
				'label': true_labels[i],
				'probability': probs[i],
				'pred_label': pred_labels[i]
			})
		return pd.DataFrame(rows)
	
	def visualize_all_tests(self, prob, pred):
		# 原始dict：{'file_name': file_name, 'timestamps/curTime_of_UTC8': timestamp_list, 'label': y, 'test_length': test_length}
		# 构建完整的dict 
		start_idx = 0
		for item in self.test_info:
			orig_len = item['test_length']                      # 该文件原始样本数（含标签）
			pred_len = orig_len - self.sequence_length          # 该文件可预测的样本数

			# 初始化完整长度的数组（前 seq_len 个为默认值 0.5, 模型没有预测）
			full_prob = np.full(orig_len, 0.5, dtype=float)     # 预测概率
			full_class = np.full(orig_len, 0.5, dtype=float)    # 预测类别（浮点型，默认0.5）

			if pred_len > 0:
				end_idx = start_idx + pred_len
				# 将模型输出的有效预测填入后半部分
				full_prob[self.sequence_length:] = prob[start_idx:end_idx].flatten()
				# 预测类别原为 int 0/1，赋值给 float 数组会自动转换
				full_class[self.sequence_length:] = pred[start_idx:end_idx].flatten()
				start_idx = end_idx

			# 写回字典
			item['probability'] = full_prob
			item['pred_label'] = full_class
			df = self.build_dataframe_from_test_info(item)
			
			# 可视化接口
			# plot_output_for_models(df)
		# dict：{'file_name': file_name, 'timestamps/curTime_of_UTC8': timestamp_list, 'label': y, 'test_length': test_length, 'y_prob': full_prob, 'y_pred': full_class}
	
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
		self.visualize_all_tests(y_pred, y_pred_classes)
		
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
	# 指定包含CSV文件的目录路径
	directory_path = '.'  # 当前目录
	used_seed = 345253545

	# 创建并训练LSTM模型
	lstm_model = LSTMModel()
	
	# 加载多个文件
	print("正在加载多个文件数据...")
	load_files.load_multiple_files(directory_path, used_seed, lstm_model)
	
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