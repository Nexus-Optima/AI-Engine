import matplotlib.pyplot as plt

threshold = 0.5


def evaluate_spearman_correlation_analysis(data):
    spearman_correlations = data.corr(method='spearman')['Output'].sort_values(ascending=False)
    spearman_correlations.drop('Output', inplace=True)

    spearman_correlations.sort_values().plot(kind='barh', figsize=(10, 8))
    plt.title('Correlation of Features with Output')
    plt.xlabel('Correlation Coefficient')
    plt.show()

    selected_features = spearman_correlations[spearman_correlations.abs() > threshold].index.tolist()
    selected_df = data[['Output'] + selected_features]

    return selected_df
