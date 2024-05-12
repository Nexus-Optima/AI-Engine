import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose


def evaluate_time_series_decomposition(data, max_lags=30):
    result = seasonal_decompose(data['Output'], model='additive', period=365)

    def lagged_correlation(series1, series2, max_lags):
        correlations = []
        for lag in range(0, max_lags + 1):
            if lag == 0:
                corr = series1.corr(series2)
            else:
                corr = series1.corr(series2.shift(-lag))
            correlations.append(corr)
        return correlations

    # Function to plot each feature against trend, seasonality, and its lagged correlation
    def plot_feature_against_components_and_lags(feature):
        fig, axes = plt.subplots(3, 1, figsize=(14, 12))

        # Plot trend
        axes[0].plot(result.trend, label='Trend', color='orange')
        ax2 = axes[0].twinx()
        ax2.plot(data[feature], label=feature, color='blue', linestyle='--')
        axes[0].set_ylabel('Output Components')
        ax2.set_ylabel(feature)
        axes[0].set_title(f'{feature} vs. Trend Component')
        axes[0].legend(loc='upper left')
        ax2.legend(loc='upper right')

        # Plot seasonality
        axes[1].plot(result.seasonal, label='Seasonality', color='green')
        ax3 = axes[1].twinx()
        ax3.plot(data[feature], label=feature, color='blue', linestyle='--')
        axes[1].set_ylabel('Output Components')
        ax3.set_ylabel(feature)
        axes[1].set_title(f'{feature} vs. Seasonality Component')
        axes[1].legend(loc='upper left')
        ax3.legend(loc='upper right')

        # Plot lagged correlations
        feature_data = data[feature]
        trend_corr = lagged_correlation(result.trend.dropna(), feature_data, max_lags)
        seasonal_corr = lagged_correlation(result.seasonal, feature_data, max_lags)
        axes[2].plot(range(0, max_lags + 1), trend_corr, label='Trend', marker='o')
        axes[2].plot(range(0, max_lags + 1), seasonal_corr, label='Seasonality', marker='x')
        axes[2].axhline(0, color='grey', linestyle='--', linewidth=0.8)
        axes[2].set_title(f'Lagged Correlation of {feature} with Trend and Seasonality Components')
        axes[2].set_xlabel('Lag (Days)')
        axes[2].set_ylabel('Correlation Coefficient')
        axes[2].legend()

        plt.tight_layout()
        plt.show()

    selected_features = [col for col in data.columns if
                         col not in ['Date', 'Output', 'id'] and not col.startswith('lag_')]
    for feature in selected_features:
        plot_feature_against_components_and_lags(feature)
