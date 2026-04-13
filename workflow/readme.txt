2026.03.02
本目录设计的基本思路遵循m_worklog.md中2026.03.03中设计规范。

2026.04.12
worklow目录下的目标是汇总此前的脚本内容。
其目录结构如下：

workflow
	├──components
	│
	│	├──export_from_capFiles_01		负责样本信息汇总、原始数据导出、输出目录构建
	│	│	├──driverfile_processing.py	将样本条目读入为格式化结构
	│	│	├──storage_processing.py		根据样本条目创建输出目录
	│	│	├──export_csv_from_cap.py	将cap抓包文件导出为csv文件
	│	│	├──export_csv_from_json.py	将cap抓包文件导出为json文件(暂未启用)
	│	│	├──merge_orginal_csvFiles.py	将导出的原始csv文件进行合并
	│	│	└──merge_capFiles.py		将琐碎的cap抓包文件进行合并
	│	│
	│	├──extractor_02				负责具体的数据特征采集
	│	│	├──extractor.py			采集器的基类：采集器的行为规范
	│	│	├──extractor_container.py		采集器容器：容纳并驱动各采集器
	│	│	├──overview_extractor.py		采集器-总览信息：通过tshark工具获取总览信息，是必须执行且优先级最高的采集器
	│	│	├──localIP_extractor.py		采集器-本机IP地址：通过总览信息获取本机IP地址
	│	│	├──udp_extractor.py			采集器-UDP特征：继承于旧有版本
	│	│	├──modem_extractor.py		采集器-Modem特征：作为兼容接口
	│	│	└──setime_extractor.py		采集器-开始截至时间：优先级最低的采集器，通过其他采集器的信息判断样本的开始截至时间
	│	│
	│	├──combiner_03				负责多类特征采集结果的合并
	│	│	├──combiner.py			合并器的基类：采集器的行为规范
	│	│	├──combiner_driver.py		合并器驱动器：容纳并驱动各合并器工作
	│	│	└──modem_combiner.py		合并器-Modem：将Modem特征向主轴数据矩阵合并
	│	│
	│	├──label_04					为合并后数据打上标签，并根据开始截至时间执行数据矩阵切割
	│	│	└──labeler.py				标签工具：执行打标签、切割开始截至区间工作
	│	│
	│	├──model_05					模型库，等待后续更新
	│	│	└──Wating-for-Updating
	│	│
	│	├──visual_06					负责数据特征的可视化
	│	│	└──udp_visual.py			可视化-UDP特征：可视化UDP特征
	│	│
	│	├──lib						其他库
	│	│	└──src					Modem特征相关，继承于福持的代码
	│	│
	│	├──utils						组件库-辅助组件的集中处
	│	│	└──read_lagList.py			提供卡顿区间列表的读取
	│	│
	│	├──main.py					主函数，通过主函数驱动完整工作流
	│	├──visual.py					可视化函数，对已有样本执行可视化工作
	│	├──targetList.txt				目标列表-完整继承于address_list.txt文件
	│	└──targetList_test.txt			测试用的目标列表
	│
	├──output						输出目录
	├──test							测试目录
	└──readme.txt

真实目录下将会有诸多文件并未在上表中标明，这些文件是正在等待更新和处理的文件，你可以暂时忽略他们。

使用：
一、主流程
1. 主流程的指令格式
	使用指令：
	cd components
	python main.py [command ... ...] <targetList.txt path>
	示例：python main.py -o -l -u -se ./targetList.txt

2. 指令
提取器指令：
	-o	必须指令，表示总览提取器overview_extractor的纳入
	-l	必须指令，表示本机地址提取器localIP_extractor的纳入
	-u	可选指令，表示UDP特征提取器udp_extractor的纳入
	-m	可选指令，表示modem特征提取modem_extractor的纳入
	-se	可选指令，表示开始截至时间提取器setime_extractor的纳入
合并器指令：
	-cm	可选指令，表示modem数据向主轴数据合并的合并器纳入

二、可视化
1. 可视化的指令格式
	使用指令：
	cd components
	python visual.py [commands ... ...] [ /path/to/flow_<date>_<time>/<scene>_<id>]
	示例：python .\visual.py -u ..\output\flow_20260408_173634\video_20251031050010\

2. 指令
可视化的选择：
	-u	可选指令，执行UDP特征的可视化