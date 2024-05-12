from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import RFE
import xgboost as xgb
import pandas as pd


def evaluate_recursive_feature_elimination(data, n_features_to_select=10):
    X = data.drop(columns='Output')
    y = data['Output']

    models = [
        ("Linear Regression", LinearRegression()),
        ("Decision Tree", DecisionTreeRegressor(random_state=42)),
        ("Random Forest", RandomForestRegressor(n_estimators=50, random_state=42)),
        ("XGBoost", xgb.XGBRegressor(n_estimators=50, max_depth=5, n_jobs=-1, objective='reg:squarederror', random_state=42))
    ]
    ranking_df = pd.DataFrame({'Feature': X.columns})

    for model_name, model in models:
        selector = RFE(model, n_features_to_select=n_features_to_select, step=1)
        selector = selector.fit(X, y)
        ranking_df[model_name] = selector.ranking_

    ranking_df['Average_Rank'] = ranking_df.mean(axis=1)

    ranking_df = ranking_df.sort_values(by='Average_Rank')

    selected_features = ranking_df.head(n_features_to_select)['Feature'].tolist()
    selected_df = data[['Output'] + selected_features]

    print(selected_df)

    return selected_df
