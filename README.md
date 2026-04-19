# 历史消息：
2025.10.28
	该仓库的目标在于为华科UDP项目，一方面，其作为远程仓库，将提供分布式开发支持；另一方面，该仓库将为项目解决方案、验证方案提供初步的版本管理支持。

# 项目目录情况：
<pre>
├── data_Precessing		 # UDP特征提取的老版本脚本
├── Dataset_of_Wuhan	 # 存放武汉数据集的相关文件
│   ├── data/processed   # 经脚本处理过后的结果
│   │   ├── modem_info
│   │   ├── combine_info 	# 融合数据集
│   │   ├── input_model     # 根据过滤脚本选择好特征，直接投入模型
│   │   └── pcap_info	 	# UDP数据集
│   └── data_label   	 # 标签文件
├── src					 # 本分支提供的代码
├── 其他README文件
└── .gitignore
</pre>