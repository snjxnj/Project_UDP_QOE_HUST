2026.03.20
combiner的目标是将各个各个数据特征下的数据矩阵进行合并；

由于来源于不同数据方向的数据信息，在时间尺度上的规范不同，我们暂定以UDP协议、TCP协议下传输层构建的时间戳为基准，构建合并标准。
由于来源于不同数据方向的数据信息，需要不同的时间对其方法，即可能需要不同的combiner实现，所以，
在总设计规范上，combiner结构如下：
combiner_driver
    combiner驱动器，内部包含：
    commands:指令容器，用于容纳指令信息，确定被合并的内容
    combiner_pool：容器，用于容纳各个combiner，在初始化时，根据指令信息初始化既定的combiner

combiner
    combiner具体实现的虚父类

***_combiner
    继承于combiner，作为实际combine方法实现确切的合并任务


modem_combiner