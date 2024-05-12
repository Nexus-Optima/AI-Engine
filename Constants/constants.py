from Feature_Selection.Correlation_Analysis import evaluate_correlation_analysis
from Feature_Selection.Spearman_Correlation import evaluate_spearman_correlation_analysis

from Models.XG_Boost.adaptive_xgboost import execute_adaptive_xgboost
from Models.ETS import execute_ets
from Models.Arima import execute_arima

from DL_Models.LSTM.LSTM import execute_lstm

import os

correlation_methods = [evaluate_correlation_analysis, evaluate_spearman_correlation_analysis]

forecasting_models = [execute_adaptive_xgboost, execute_arima, execute_ets, execute_lstm]


class Commodities:
    COMMODITIES = "b3llcurve-commodities"
    FORECAST_STORAGE = "b3llcurve-forecast-storage"
    COTTON = "cotton"


class Credentials:
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")


class News:
    API_KEY = os.getenv("NEWS_API_KEY")
