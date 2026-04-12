import torch
import numpy
from torch import nn, Tensor, optim
import torch.nn.functional as F
import matplotlib.pyplot as plt

class Binary_classification(nn.Module):
	def __init__(self, input_size, num_hidden, output_size):
		super(Binary_classification, self).__init__()
		self.hidden = nn.Linear(input_size, num_hidden)
		self.output = nn.Linear(num_hidden,output_size)
	
	def forward(self, x):
		x = F.relu(self.hidden(x))
		x = self.output(x)
		return x

def show_res(inputs:Tensor, targets:Tensor, outputs:Tensor, loss, epoch):
	inputs = inputs.cpu()
	targets = targets.cpu()
	outputs = outputs.cpu()

	plt.clf()
	pred_y = torch.max(outputs, 1)[1].data.numpy().squeeze()
	y = targets.numpy()

	# 第1个子图：模型预测
	plt.subplot(1, 2, 1)
	plt.scatter(inputs.numpy()[:,0], inputs.numpy()[:,1], c=pred_y)
	plt.title(f'epoch={epoch} preb label')
	Accuracy = (pred_y == y).sum() / len(y)
	plt.text(0.05, 0.95, f'Acc={Accuracy:.4f}', color='red', fontsize=12,
		transform=plt.gca().transAxes, verticalalignment='top')

	# 第2个子图：真实标签
	plt.subplot(1, 2, 2)
	plt.scatter(inputs.numpy()[:,0], inputs.numpy()[:,1], c=y)
	plt.title('true label')

	plt.tight_layout()
	plt.pause(0.5)

def train(model:nn.Module, inputs:Tensor, targets:Tensor, epochs:int):
	# 优化器/损失函数
	optimizer = optim.SGD(model.parameters(), lr=0.001)
	criterion = nn.CrossEntropyLoss()

	for epoch in range(epochs):
		outputs = model.forward(inputs)
		loss = criterion(outputs, targets)
		optimizer.zero_grad()
		loss.backward()
		optimizer.step()

		if epoch % 10 == 0:
			show_res(inputs, targets, outputs, loss, epoch)

	return loss

def main():
	# 数据生成
	point = torch.ones(500, 2)
	class0 = torch.normal(4*point, 2)
	class1 = torch.normal(-4*point, 2)
	label0 = torch.zeros(500, 1)
	label1 = torch.ones(500, 1)

	x = torch.cat([class0,class1],).type(torch.FloatTensor)
	y = torch.cat([label0,label1],).type(torch.LongTensor).squeeze()
	
	# 检验设备/生成数据集
	have_CUDA = torch.cuda.is_available()
	if have_CUDA:
		print('used device:cuda')
		BC_net = Binary_classification(input_size=2, num_hidden=20, output_size=2).cuda()
		inputs = x.cuda()
		targets = y.cuda()
	else:
		print('used device:cpu')
		BC_net = Binary_classification(input_size=2, num_hidden=20, output_size=2)
		inputs = x
		targets = y
	# 训练
	train(BC_net, inputs, targets, epochs=1000)
		
if __name__ == '__main__':
	main()