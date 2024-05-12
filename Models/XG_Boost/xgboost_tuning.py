import optuna
from sklearn.metrics import mean_absolute_error, mean_squared_error
import xgboost as xgb


def execute_evaluation(subset_data, hyperparams):
    predictions = []
    actual_values = []
    window_size = int(0.5 * len(subset_data))

    model = xgb.XGBRegressor(
        n_estimators=hyperparams['n_estimators'],
        max_depth=hyperparams['max_depth'],
        learning_rate=hyperparams['learning_rate'],
        subsample=hyperparams['subsample'],
        colsample_bytree=hyperparams['colsample_bytree'],
        n_jobs=-1,
        objective='reg:squarederror',
        random_state=42,
        early_stopping_rounds=50
    )

    # Moving window approach to train and validate the model
    for window_start in range(0, len(subset_data) - window_size):
        train_data = subset_data.iloc[window_start:window_start + window_size]
        val_data = subset_data.iloc[window_start + window_size:window_start + window_size + 1]

        X_train, y_train = train_data.drop(columns='Output'), train_data['Output']
        X_val, y_val = val_data.drop(columns='Output'), val_data['Output']

        model.fit(X_train, y_train, eval_set=[(X_train, y_train), (X_val, y_val)])

        predictions.extend(model.predict(X_val))
        actual_values.extend(y_val.values)

    print(mean_squared_error(actual_values, predictions))
    print(mean_absolute_error(actual_values, predictions))

    return actual_values, predictions


def objective(trial, data):
    hyperparams = {
        'n_estimators': trial.suggest_int("n_estimators", 25, 500),
        'max_depth': trial.suggest_int("max_depth", 1, 18),
        'learning_rate': trial.suggest_float("learning_rate", 1e-8, 1e-1, log=True),
        'subsample': trial.suggest_float("subsample", 0.1, 1),
        'colsample_bytree': trial.suggest_float("colsample_bytree", 0.1, 1)
    }

    actual_values, predictions = execute_evaluation(data, hyperparams)
    mse = mean_squared_error(actual_values, predictions)

    # Store additional information
    trial.set_user_attr('actual_values', actual_values)
    trial.set_user_attr('predictions', predictions)

    return mse


def tune_xgboost_hyperparameters(data, n_trials=10):
    study = optuna.create_study(direction="minimize")
    study.optimize(lambda trial: objective(trial, data), n_trials=n_trials)

    # Retrieve the best trial's additional attributes
    best_trial = study.best_trial
    best_actual_values = best_trial.user_attrs['actual_values']
    best_predictions = best_trial.user_attrs['predictions']

    return study.best_params, best_actual_values, best_predictions
