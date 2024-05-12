import pandas as pd
from sklearn.decomposition import PCA
import numpy as np


def execute_pca(data):
    features = data.drop(columns=["Output"])
    pca = PCA(n_components=10)
    pca.fit(features)

    train_size = int(0.8 * len(data))
    train, test = data.iloc[:train_size], data.iloc[train_size:]

    X_train, X_test = train.drop("Output", axis=1), test.drop("Output", axis=1)
    y_train, y_test = train["Output"], test["Output"]
    loadings = pca.components_.T
    loading_df = pd.DataFrame(loadings, columns=[f"PC{i + 1}" for i in range(pca.n_components_)], index=X_train.columns)
    print(loading_df)

    cumulative_variance = np.cumsum(pca.explained_variance_ratio_)
    num_components_95 = np.where(cumulative_variance >= 0.95)[0][0] + 1
    print(num_components_95)

    pca_values_df = pd.DataFrame()

    for i in range(num_components_95):
        loadings = pca.components_[i]
        pc_values = features.dot(loadings)
        pca_values_df[f"PC{i + 1}"] = pc_values
    pca_values_sorted = pca_values_df.sort_index()
    pca_values_sorted.groupby(pca_values_sorted.index).mean()
