from sklearn.ensemble import RandomForestRegressor
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

threshold_percentage = 50


def evaluate_tree_based_models(data):
    X = data.drop(columns='Output')
    y = data['Output']

    rf = RandomForestRegressor(n_estimators=100, random_state=20)
    rf.fit(X, y)

    feature_importances = rf.feature_importances_

    importance_df = pd.DataFrame({
        'Feature': X.columns,
        'Importance': feature_importances
    }).sort_values(by='Importance', ascending=False)

    plt.figure(figsize=(12, 6))
    sns.barplot(data=importance_df, x='Importance', y='Feature', color='skyblue')
    plt.title('Feature Importance from Random Forest')
    plt.xlabel('Importance Score')
    plt.ylabel('Features')
    plt.tight_layout()
    plt.show()

    max_importance = importance_df['Importance'].max()
    threshold = (threshold_percentage / 100) * max_importance
    selected_features = importance_df[importance_df['Importance'] > threshold]['Feature'].tolist()

    selected_df = data[['Output'] + selected_features]

    return selected_df
