xgboost_params_2Y = {'n_estimators': 10, 'max_depth': 5, 'learning_rate': 0.09750500973354762,
                     'subsample': 0.8462469506142297, 'colsample_bytree': 0.8975002288508139}

xgboost_params_4Y = {'n_estimators': 10, 'max_depth': 9, 'learning_rate': 0.03405165565562194,
                     'subsample': 0.4722411290242451, 'colsample_bytree': 0.15968830393567798}

# lstm_parameters_4Y_30D = {'lr': 0.010078437366717426, 'num_layers': 1, 'hidden_size': 10, 'dropout': 0.30000000000000004,
#                       'weight_decay': 1.6531586375007128e-05, 'num_epochs': 950}
#
# lstm_parameters_4Y_15D = {'lr': 0.0041901813503098006, 'num_layers': 1, 'hidden_size': 110, 'dropout': 0.2,
#                           'weight_decay': 8.242010681316957e-05, 'num_epochs': 950}
#
# lstm_parameters_4Y_7D = {'lr': 0.03747160128201738, 'num_layers': 1, 'hidden_size': 10, 'dropout':  0.30000000000000004,
#                          'weight_decay': 1.4758609071824746e-05, 'num_epochs': 900}

lstm_parameters_days = [{'lr': 0.03747160128201738, 'num_layers': 1, 'hidden_size': 10, 'dropout':  0.30000000000000004,
                            'weight_decay': 1.4758609071824746e-05, 'num_epochs': 900},

                        {'lr': 0.0041901813503098006, 'num_layers': 1, 'hidden_size': 110, 'dropout': 0.2,
                            'weight_decay': 8.242010681316957e-05, 'num_epochs': 950},

                        {'lr': 0.010078437366717426, 'num_layers': 1, 'hidden_size': 10, 'dropout': 0.30000000000000004,
                            'weight_decay': 1.6531586375007128e-05, 'num_epochs': 950}]

lstm_parameters_4Y_4W = {'lr': 0.04820743142687317, 'num_layers': 1, 'hidden_size': 80, 'dropout': 0.30000000000000004,
                         'weight_decay': 0.001086373193268082, 'num_epochs': 500}

lstm_parameters_4Y_8W = {'lr': 0.0414811928949997, 'num_layers': 2, 'hidden_size': 90, 'dropout': 0.2,
                         'weight_decay': 0.00092165975509461, 'num_epochs': 1000}

lstm_parameters_4Y_12W = {'lr': 0.05462826488179377, 'num_layers': 1, 'hidden_size': 30, 'dropout': 0.2,
                          'weight_decay': 0.00015779019258137665, 'num_epochs': 1000}

FORECASTING_DAYS = [7,15,30]

FORECASTING_WEEKS = [4,8,12]

