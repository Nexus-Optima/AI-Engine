import pandas as pd
from Models.XG_Boost.adaptive_xgboost import execute_evaluation, execute_adaptive_xgboost
from DL_Models.LSTM.LSTM import execute_lstm
from Utils.process_data import process_data_lagged_rolling_stats
import Constants.parameters as prms


def execute_purchase_strategy_v2(raw_data, data, dailyConsumption, maxDays, forecast_days):
    actual_values, predictions, forecast = execute_lstm(raw_data, data, forecast_days, prms.lstm_parameters_4Y_15D)

    def optimum_purchase(pred):
        curStock = 0
        stockStatement = []

        for i in range(0, len(pred)):
            current = pred[i]
            for j in range(i, len(pred)):
                if pred[j] < current or j == len(pred) - 1:
                    if maxDays < j - i:
                        curStock = maxDays
                    else:
                        if curStock < j - i:
                            curStock = j - i
                    stockStatement.append(curStock * dailyConsumption)
                    break
            curStock -= 1
        return stockStatement

    def compute_purchase_cost(stockStatement):
        stockToBuy = stockStatement[0]
        purchaseCost = [actual_values[0] * stockToBuy]
        for i in range(0, len(stockStatement) - 1):
            # print(len(stockStatement)-1)
            if stockStatement[i] <= stockStatement[i + 1]:
                stockToBuy = stockStatement[i + 1] - stockStatement[i] + 1 * dailyConsumption
                purchaseCost.append(stockToBuy * actual_values[i + 1])
                # print("Buy " + str(stockStatement[i + 1] - stockStatement[i] + 1 * dailyConsumption))
        return purchaseCost

    predStockStatement = optimum_purchase(predictions)
    predPurchaseCost = compute_purchase_cost(predStockStatement)
    actualStockStatement = optimum_purchase(actual_values)
    actualPurchaseCost = compute_purchase_cost(actualStockStatement)
    forecastPurchase = optimum_purchase(forecast)
    print(forecastPurchase)
    print("Predicted Purchase cost is " + str(sum(predPurchaseCost)))
    print("Optimum Purchase cost is " + str(sum(actualPurchaseCost)))
    print("error is " + str((sum(predPurchaseCost) - sum(actualPurchaseCost)) / sum(actualPurchaseCost)))
