# label.txt文件统计脚本
这个脚本用于可视化统计data_label目录中的所有label.txt文件的卡顿时长、卡顿次数

## 使用这个脚本：
### 目录结构结构：
project/

├── data_label/           # label.txt存放文件目录

├── label_log_analyzer/   # label分析脚本所在目录

保证这两个文件夹同级即可

### 库：
matplotlib

### 想做修改？
更改pic_highlight_diff_cases()函数中的内容即可手动标注背景区间

## 效果概览：

### 卡顿次数
![图片1描述](./label_log_analyzer/result_pic/lag_counts.png)

### 卡顿总时长
![图片1描述](./label_log_analyzer/result_pic/lag_total_seconds.png)
