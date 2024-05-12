import torch.nn as nn
import torch


class LSTMModel(nn.Module):
    def __init__(self, input_size, hyperparameters):
        super(LSTMModel, self).__init__()
        self.hidden_size = hyperparameters['hidden_size']
        self.num_layers = hyperparameters['num_layers']

        self.lstm = nn.LSTM(input_size, self.hidden_size, self.num_layers,
                            dropout=hyperparameters['dropout'], batch_first=True)
        self.fc = nn.Linear(self.hidden_size, 1)

    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).requires_grad_()
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).requires_grad_()

        out, (hn, cn) = self.lstm(x, (h0.detach(), c0.detach()))
        out = self.fc(out[:, -1, :])
        return out
