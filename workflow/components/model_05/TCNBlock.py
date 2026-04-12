"""
TCNBlock.py:
TCNBlock是一个实现了Temporal Convolutional Network（TCN）架构的Keras层。TCN是一种用于序列建模的深度学习架构，它通过堆叠膨胀卷积层来捕获长距离依赖关系。
TCNBlock类具有以下主要功能：
1. **卷积层**：使用Conv1D层实现膨胀卷积，能够捕获不同时间步长的依赖关系。
2. **激活函数**：支持多种激活函数，如ReLU、LeakyReLU等。
3. **正则化**：通过Dropout、Batch Normalization和Layer Normalization等技术来防止过拟合。
4. **残差连接**：通过Add层实现残差连接，有助于模型的训练和收敛。
5. **输出层**：通过Lambda层实现输出层的计算，可以自定义输出函数。
6. **可训练参数**：通过可训练参数控制卷积核大小、膨胀系数、激活函数等。
"""

import tensorflow as tf
from keras.layers import Layer, Conv1D, Dropout, SpatialDropout1D, Activation, Lambda, Add, BatchNormalization, LayerNormalization
from keras import backend as K

class ResidualBlock(tf.keras.layers.Layer):

    def __init__(self, filters, kernel_size, dilation_rate, dropout_rate=0.2):
        super(ResidualBlock, self).__init__()

        self.conv1 = tf.keras.layers.Conv1D(
            filters,
            kernel_size,
            padding='causal',
            dilation_rate=dilation_rate
        )

        self.conv2 = tf.keras.layers.Conv1D(
            filters,
            kernel_size,
            padding='causal',
            dilation_rate=dilation_rate
        )

        self.activation = tf.keras.layers.Activation('relu')
        self.dropout = tf.keras.layers.SpatialDropout1D(dropout_rate)

        self.downsample = None

    def build(self, input_shape):

        if input_shape[-1] != self.conv1.filters:
            self.downsample = tf.keras.layers.Conv1D(
                self.conv1.filters,
                1,
                padding='same'
            )

    def call(self, x):

        residual = x

        y = self.conv1(x)
        y = self.activation(y)
        y = self.dropout(y)

        y = self.conv2(y)
        y = self.activation(y)
        y = self.dropout(y)

        if self.downsample is not None:
            residual = self.downsample(residual)

        return tf.keras.layers.add([y, residual])
    
class TCN(tf.keras.layers.Layer):

    def __init__(self,
                 filters=64,
                 kernel_size=3,
                 dilations=(1,2,4,8),
                 dropout_rate=0.2):

        super(TCN, self).__init__()

        self.blocks = []

        for d in dilations:

            self.blocks.append(
                ResidualBlock(
                    filters=filters,
                    kernel_size=kernel_size,
                    dilation_rate=d,
                    dropout_rate=dropout_rate
                )
            )

    def call(self, x):

        for block in self.blocks:
            x = block(x)

        return x
'''
class TCNBlock(Layer):
    """
    Temporal Convolutional Network (TCN) Block

    TCN是一种用于序列建模的深度学习架构，它通过堆叠膨胀卷积层来捕获长距离依赖关系。

    参数:
        input_shape: 输入形状，格式为(sequence_length, features)
        filters: 卷积核数量
        kernel_size: 卷积核大小
        dilations: 膨胀率列表，控制卷积的感受野
        activation: 激活函数
        dropout_rate: dropout率
        use_batch_norm: 是否使用批归一化
        use_layer_norm: 是否使用层归一化
        use_weight_norm: 是否使用权重归一化
        kernel_initializer: 卷积核初始化器
        padding: 填充方式，'causal'或'same'
    """

    def __init__(self, 
                 input_shape=None,
                 filters=64,
                 kernel_size=3,
                 dilations=[1, 2, 4, 8],
                 activation='relu',
                 dropout_rate=0.2,
                 use_batch_norm=False,
                 use_layer_norm=False,
                 use_weight_norm=False,
                 kernel_initializer='he_normal',
                 padding='causal',
                 **kwargs):
        super(TCNBlock, self).__init__(**kwargs)
        self.input_shape_ = input_shape
        self.filters = filters
        self.kernel_size = kernel_size
        self.dilations = dilations
        self.activation = activation
        self.dropout_rate = dropout_rate
        self.use_batch_norm = use_batch_norm
        self.use_layer_norm = use_layer_norm
        self.use_weight_norm = use_weight_norm
        self.kernel_initializer = kernel_initializer
        self.padding = padding

        # 创建卷积层列表
        self.conv_layers = []
        self.batch_norm_layers = []
        self.layer_norm_layers = []
        self.dropout_layers = []
        self.activation_layers = []

        # 残差连接层
        self.residual_conv = None

    def build(self, input_shape):
        # 创建卷积层
        for dilation in self.dilations:
            # 1D膨胀卷积层
            conv = Conv1D(
                filters=self.filters,
                kernel_size=self.kernel_size,
                dilation_rate=dilation,
                padding=self.padding,
                kernel_initializer=self.kernel_initializer
            )
            self.conv_layers.append(conv)

            # Dropout层
            dropout = SpatialDropout1D(self.dropout_rate)
            self.dropout_layers.append(dropout)

            # 批归一化层
            if self.use_batch_norm:
                batch_norm = BatchNormalization()
                self.batch_norm_layers.append(batch_norm)

            # 层归一化层
            if self.use_layer_norm:
                layer_norm = LayerNormalization()
                self.layer_norm_layers.append(layer_norm)

            # 激活层
            activation = Activation(self.activation)
            self.activation_layers.append(activation)

        # 如果输入通道数与输出通道数不同，需要1x1卷积进行残差连接
        if input_shape[-1] != self.filters:
            self.residual_conv = Conv1D(
                filters=self.filters,
                kernel_size=1,
                padding='same',
                kernel_initializer=self.kernel_initializer
            )
            self.residual_conv.build(input_shape)

        # 构建所有层
        for conv in self.conv_layers:
            conv.build(input_shape)
            input_shape = conv.compute_output_shape(input_shape)

        super(TCNBlock, self).build(input_shape)

    def call(self, inputs, training=None):
        x = inputs

        # 如果需要，应用残差连接的1x1卷积
        residual = inputs
        if self.residual_conv is not None:
            residual = self.residual_conv(residual)

        # 应用膨胀卷积层，批归一化和激活函数
        for i in range(len(self.dilations)):
            x = self.conv_layers[i](x)

            if self.use_batch_norm:
                #  层归一化和批归一化的顺序可以根据需要调整，这里先进行批归一化
                x = self.batch_norm_layers[i](x, training=training)

            if self.use_layer_norm:
                x = self.layer_norm_layers[i](x, training=training)

            x = self.activation_layers[i](x)
            x = self.dropout_layers[i](x, training=training)

        # 添加残差连接
        x = Add()([x, residual])

        return x

    def compute_output_shape(self, input_shape):
        """
        计算输出形状，保持时间维度不变，通道数变为filters"""
        return (input_shape[0], input_shape[1], self.filters)

    def get_config(self):
        config = super(TCNBlock, self).get_config()
        config.update({
            'filters': self.filters,
            'kernel_size': self.kernel_size,
            'dilations': self.dilations,
            'activation': self.activation,
            'dropout_rate': self.dropout_rate,
            'use_batch_norm': self.use_batch_norm,
            'use_layer_norm': self.use_layer_norm,
            'use_weight_norm': self.use_weight_norm,
            'kernel_initializer': self.kernel_initializer,
            'padding': self.padding
        })
        return config
        '''

# 对此文件进行验证
if __name__ == "__main__":
    print("正在验证TCNBlock的功能...")
    # 创建一个TCNBlock实例
    tcn_block = TCN(input_shape=(100, 16), filters=32, kernel_size=3, dilations=[1, 2], activation='relu', dropout_rate=0.1)

    # 构建模型并查看输出形状
    input_data = tf.random.normal((1, 100, 16))  # 模拟输入数据
    output_data = tcn_block(input_data)
    print("输出形状:", output_data.shape)