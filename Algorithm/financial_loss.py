"""
@Params
predicted_prices -> Predictions from the models
actual_prices -> Actual prices from the industries
daily_consumption -> Daily usage for the commodity
current_stock -> Current stock hold
max_stock -> daily_consumption * max_days (Total stock hold based on financial and warehouse constraints)
"""


def execute_purchase_strategy(predicted_prices, actual_prices, daily_consumption, current_stock, max_stock):
    """
    This function determines the optimal purchase strategy based on predicted and actual prices.
    The goal is to minimize the total cost while meeting daily consumption needs.
    """

    def purchase_strategy(prices):
        """
        Determines the best days to purchase stock and the quantities to purchase.

        @Params:
        prices: The list of prices over time.

        Returns:
        days_to_buy: Days on which purchases are made.
        purchase_quantities: Quantities purchased on those days.
        """

        # Number of days for which we have price data
        n = len(prices)

        # Maximum stock + 1 for indexing purposes
        m = max_stock + 1

        # Initialize DP table to store total cost of meeting consumption needs up to day i with stock j
        DP = [[float('inf')] * m for _ in range(n)]

        # Store decisions made on day i to get stock j
        previous_decision = [[0] * m for _ in range(n)]

        # Initialize the cost on the first day for various stock levels
        for stock in range(m):
            DP[0][stock] = (stock - current_stock + daily_consumption) * prices[0]

        # Fill the DP table
        for day in range(1, n):  # Loop through every day starting from the second day (index 1)
            for stock_today in range(m):  # For each possible stock level today
                for stock_yesterday in range(m):
                    # For each possible stock level yesterday calculate the quantity that needs to be
                    # purchased today based on stock levels from yesterday and today, as well as the daily consumption.
                    purchase_today = stock_today - stock_yesterday + daily_consumption
                    # If the purchase quantity for today is valid (i.e., non-negative and within maximum stock limit)
                    if 0 <= purchase_today <= max_stock:
                        # Calculate the cost for today based on the cost from yesterday (from DP table)
                        # and the cost of purchasing today's quantity at today's prices.
                        cost = DP[day - 1][stock_yesterday] + purchase_today * prices[day]
                        # If the calculated cost is less than the existing cost in the DP table for today's stock level,
                        # then update the DP table with the new cost and store the decision (stock level from yesterday).
                        if cost < DP[day][stock_today]:
                            DP[day][stock_today] = cost
                            previous_decision[day][stock_today] = stock_yesterday

        # Backtrack to find the days to buy and the quantities to buy
        stock = 0
        days_to_buy = []
        purchase_quantities = []
        for day in range(n - 1, -1, -1):
            stock_yesterday = previous_decision[day][stock]
            purchase_today = stock - stock_yesterday + daily_consumption
            if purchase_today > 0:
                days_to_buy.append(day)
                purchase_quantities.append(purchase_today)
            stock = stock_yesterday

        return list(reversed(days_to_buy)), list(reversed(purchase_quantities))

    # Determine the purchase strategy based on predicted and actual prices
    pred_days_to_buy, pred_purchase_quantities = purchase_strategy(predicted_prices)
    ori_days_to_buy, ori_purchase_quantities = purchase_strategy(actual_prices)

    # Calculate total cost based on the derived purchase strategies
    total_predicted_price = sum(
        pred_purchase_quantities[i] * actual_prices[pred_days_to_buy[i]] for i in range(len(pred_days_to_buy)))
    total_actual_price = sum(
        ori_purchase_quantities[i] * actual_prices[ori_days_to_buy[i]] for i in range(len(ori_days_to_buy)))

    # Calculate the relative difference between the predicted and actual total costs
    result = (total_predicted_price - total_actual_price) / total_actual_price
    print(result)
