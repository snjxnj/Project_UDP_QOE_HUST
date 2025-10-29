2025.10.28
	data_PreProcessing目录下的内容是用于数据预处理、数据特征抓取、数据特征持久化、数据特征可视化的。
	在该目录结构下，你可以看到一个bat批处理文件和其他的python脚本文件，以及一个Storage目录和一个address_List.txt文件。
	这些文件的结构是这样的：
		cap_Operation.bat脚本将作为启动器，将会按照既定工作流程，在address_List.txt的指示下完成固定任务。
		address_List.txt文件将作为引导文件，记录待执行的任务信息，引导cap_Operation.bat文件逐步推进任务。
		.py文件是众多的支持脚本，旨在为cap_Operation.bat脚本的执行提供实现。
		Storage目录是存储目录，将作为每一次任务执行完毕后持久化的存储点。
	如果要使用这个脚本，那么请按照如下步骤操作：
		1. 首先编写address_List.txt文件，编写规范如下：
			ID:<ID number>,	src_Add:<soruce path>,	scene:<gaming or video or meeting>,	local_ip:<IP Address>,	serv_ip:<IP Address>,	start_time:<time string HH-MM-SS>,	end_time:<time string HH-MM-SS>,	lag_timeList_path:<list_path>
			参数含义：
				ID：待处理的样本编号，包含了样本的采样时间、处理模式、处理结果信息。
				src_Add：样本的存储路径，脚本运行要求提供样本原始数据的路径，该目录下通常包含了bbklog等目录
				scene：数据样本的场景
				local_ip：该样本下的本机IP地址，也就是手机的IP地址
				serv_ip：该样本下的服务器IP地址，也就是游戏服务、视频服务、会议服务的IP通信地址
				start_time：该样本下可靠数据的起始时间(实际样本可能包含了测试开始的准备阶段和测试结束的导出阶段，我们不需要这些数据内容，进行人为的起始时间划定，从而过滤无用数据)
				end_time：该样本下可靠数据的截至时间(实际样本可能包含了测试开始的准备阶段和测试结束的导出阶段，我们不需要这些数据内容，进行人为的截至时间划定，从而过滤无用数据)
				lag_timeList_path：卡顿时间表的存储地址
			填写规范：
				ID number: 15位数字
				source path: 在Windows系统的文件管理器中右键目标文件，点击复制文件路径，粘贴于此即可。
				scene：video、meeting、gaming模式
				local_ip：支持单地址或多个地址，单地址：10.10.10.10或者240a:4abc::::::1，多地址：10.10.10.10+240a:4abc::::::1，使用+来连接，支持多个地址连接
				serv_ip：支持单地址或多个地址，单地址：255.255.255.255或者240a:4abc::::::1，多地址：255.255.255.255+240a:4abc::::::1，使用+来连接，支持多个地址连接
				start_time：HH-MM-SS，表明时分秒
				end_time：HH-MM-SS，表示时分秒
				lag_timeList_path：在Windows系统的文件管理器中右键目标文件，点击复制文件路径，粘贴于此即可。
			ID:202510170100110,	src_Add:"D:\General_Workspace\Workspace-of-UDP-NEW\DataDir\test_gaming_2025101701\export_Time20251017_211351",	scene:gaming,	local_ip:10.183.29.9,	serv_ip:111.31.245.62,  start_time:20-59-30,    end_time:21-08-45,  lag_timeList_path:"D:\General_Workspace\Workspace-of-UDP-NEW\DataDir\test_gaming_2025101701\lag_timeList.txt"
		
		2. 其次编写lag_timeList_path.txt文件
			lag_timeList.txt文件的标准
			命名标准：
				<样本名>_lag_timeList.txt
			内容标准：
				文件中每一行都是一个卡顿区间，填写规范为：
					HH:MM:SS.xxx-HH:MM:SS.xxx
			卡顿的定义标准：
				游戏环境：游戏延迟大于200ms的时间区间
				视频和会议：画面时间偏差超过2s的起始时刻和画面偏差恢复的时刻构建为一个卡顿区间。
		
		3. 确保存储目录的存在
			请务必确保在执行文件cap_Operation.bat文件的同目录下有Storage目录的存在。
			该目录的主要目标，是给脚本文件的持久化操作提供空间。
		
		4. 环境预备：
			进入Windows系统的cmd：
				(1) 在搜索栏搜索Windows Powershell，启动之后，输入cmd，即可进入Windows Powershell下的cmd子模块。
				(2) 使用Win+R，输入cmd，然后确定执行，进入系统cmd
			将cmd的编码格式调整为UTF-8编码
				在cmd命令行中输入指令：chcp 65001
		
		5. 开始执行
			在确保上述步骤完成之后，便可以准备启动脚本了，输入指令：./cap_Operation.bat 之后，可以看到指定的输出内容。
		
		6. 结果查询
			当脚本运行完毕，可以在Storage目录当中，你可以看到一个新创建的目录，目录的命名遵循YYYYMMDDHHMMSS。
			假设address_List.txt文件中记录了10个样本案例，那么新创建的目录下就会有10个目录得到创建。
			在各个目录下，都存储了cap_Operation脚本处理过程每一步的持久化，可供日后历史回溯；同时存储了可视化的png格式图片；并且存储了日后提供给模型训练的数据特征矩阵。