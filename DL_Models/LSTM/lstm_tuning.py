import torch
import torch.nn as nn
import optuna
import numpy as np
from sklearn.model_selection import train_test_split
from torch import optim

from DL_Models.LSTM.lstm_structure import LSTMModel
from DL_Models.LSTM import lstm_utils


def objective(trial, data):
    learning_rate = trial.suggest_float("lr", 1e-4, 1e-1, log=True)
    num_layers = trial.suggest_int("num_layers", 1, 5)
    hidden_size = trial.suggest_int("hidden_size", 10, 150, step=10)
    dropout = trial.suggest_float("dropout", 0.1, 0.5, step=0.1)
    weight_decay = trial.suggest_float("weight_decay", 1e-5, 1e-1, log=True)
    num_epochs = trial.suggest_int("num_epochs", 200, 1000, step=50)

    hyperparameters = {'hidden_size': hidden_size, 'num_layers': num_layers, 'dropout': dropout}

    scaled_data, data_min, data_max = lstm_utils.min_max_scaler(data.values)
    train_data, test_data = train_test_split(scaled_data, test_size=0.2, shuffle=False)
    look_back = 7
    X_train, y_train = lstm_utils.create_dataset(train_data, look_back)
    X_test, y_test = lstm_utils.create_dataset(test_data, look_back)

    X_train_tensor = torch.FloatTensor(X_train)
    y_train_tensor = torch.FloatTensor(y_train)
    X_test_tensor = torch.FloatTensor(X_test)
    y_test_tensor = torch.FloatTensor(y_test)

    # Model, Loss, Optimizer
    model = LSTMModel(input_size=X_train.shape[2], hyperparameters=hyperparameters)
    criterion = nn.MSELoss(reduction='mean')
    optimizer = optim.Adam(model.parameters(), lr=learning_rate, weight_decay=weight_decay)

    # Training Loop
    for epoch in range(num_epochs):
        model.train()
        optimizer.zero_grad()
        outputs = model(X_train_tensor)
        loss = criterion(outputs, y_train_tensor.unsqueeze(1))
        loss.backward()
        optimizer.step()

    # Evaluation
    model.eval()
    test_predictions = model(X_test_tensor).detach().numpy()
    test_predictions_orig = lstm_utils.inverse_min_max_scaler(test_predictions, data_min[0], data_max[0])
    y_test_orig = lstm_utils.inverse_min_max_scaler(y_test, data_min[0], data_max[0])
    y_test_orig = np.reshape(y_test_orig, (-1,1))
    test_mse = np.mean((test_predictions_orig - y_test_orig) ** 2)

    return test_mse


def tune_lstm_hyperparameters(data, no_trials):
    study = optuna.create_study(direction="minimize")
    study.optimize(lambda tri: objective(tri, data), n_trials=no_trials)
    best_params = study.best_trial.params
    return best_params
