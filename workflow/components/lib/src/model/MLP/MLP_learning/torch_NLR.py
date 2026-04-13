import torch    
from torch import nn,optim,Tensor
import torch.nn.functional as F
import matplotlib.pyplot as plt
# 使用三层神经网络拟合三次多项式 并动态展示其学习过程 

class NLR(nn.Module):
	def __init__(self, input_size, num_hidden, output_size):
		super(NLR, self).__init__()
		self.hidden1 = nn.Linear(input_size, num_hidden)
		self.hidden2 = nn.Linear(num_hidden, num_hidden)
		self.hidden3 = nn.Linear(num_hidden, num_hidden)
		self.output = nn.Linear(num_hidden, output_size)

	def forward(self, x):
		x = F.relu(self.hidden1(x))
		x = F.relu(self.hidden2(x))
		x = F.relu(self.hidden3(x))
		x = self.output(x)
		return x

def show_res(inputs:Tensor, targets:Tensor, outputs:Tensor, loss, epoch):
	inputs = inputs.cpu()
	targets = targets.cpu()
	outputs = outputs.cpu()

	plt.cla()
	plt.scatter(inputs.detach().numpy(), outputs.detach().numpy(), c = 'r')
	plt.plot(inputs.detach().numpy(), targets.detach().numpy(), 'g-', lw=5)
	plt.title(f'epoch={epoch}')
	# loss：
	plt.text(0.05, 0.95, f'loss={loss.item():.4f}', color='red', fontsize=16,
		transform=plt.gca().transAxes, verticalalignment='top')

	# plt.show()
	plt.pause(0.5)

def train(model:nn.Module, inputs:Tensor, targets:Tensor, epochs:int):
	# 优化器/损失函数
	optimizer = optim.SGD(model.parameters(), lr=0.001)
	criterion = nn.MSELoss()

	for epoch in range(epochs):
		outputs = model.forward(inputs)
		loss = criterion(outputs, targets)
		optimizer.zero_grad()
		loss.backward()
		optimizer.step()

		if epoch % 1000 == 0:
			show_res(inputs, targets, outputs, loss, epoch)

	return loss

def main():
	# 数据生成 x^3+0.5x^2+x+0.3
	x = torch.unsqueeze(torch.linspace(-5,5,10000), dim=1)
	print(x)
	y = x.pow(3) + 0.5 * x.pow(2) + x + 0.3*torch.rand(x.size())

	# 检验设备/生成数据集
	have_CUDA = torch.cuda.is_available()
	if have_CUDA:
		print('used device:cuda')
		NLR_net = NLR(input_size=1, num_hidden=20, output_size=1).cuda()
		inputs = x.cuda()
		targets = y.cuda()
	else:
		print('used device:cpu')
		NLR_net = NLR(input_size=1, num_hidden=20, output_size=1)
		inputs = x
		targets = y
	# 训练
	train(NLR_net, inputs, targets, epochs=50000)
	
	for name, param in NLR_net.named_parameters():
		print(f"{name}: shape={param.shape}")
		print(param)
		
if __name__ == '__main__':
	main()