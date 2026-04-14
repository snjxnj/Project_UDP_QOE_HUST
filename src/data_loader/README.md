# load_files.py使用说明
## 外层调用脚本的修改
直接在原有脚本main函数逻辑中加入：
lstm_model.X_train, lstm_model.y_train, lstm_model.X_val, lstm_model.y_val, lstm_model.X_test, lstm_model.y_test = load_files.load_multiple_files(directory_path, seed, lstm_model.sequence_length)
此外，模型build方法中：
self.model.add(LSTM(128, return_sequences=True, input_shape=(self.sequence_length, len(self.feature_columns))))
修改为：（数据特征的数量从训练集的形状（total_num, seq_len, n_features）中获取）
self.model.add(LSTM(128, return_sequences=True, input_shape=(self.sequence_length, X_train.shape[2])))