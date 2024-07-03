import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from statsmodels.tsa.stattools import grangercausalitytests
from statsmodels.tsa.stattools import adfuller


def granger_causality_test(df, target_var:str, max_lag:int=6, p_thresh:float=0.05, annot:bool=False,
                           plt_show:bool=True):
    """
    Calculate Granger Causality Test for all variables in a dataframe. Variables are individually checked for
    stationarity using ADF and differenced if unit root.

    Parameters:
        - **df**: (pd.DataFrame) Input dataframe containing at least two variables, one of which must be the target var.
        - **target_var**: (str) Column name of target var in df.
        - **max_lag**: (int) Number of lags over which Granger algorithm iterates. Defaults to 6.
        - **p_thresh**: (float) P-Value threshold for ADF test to reject null hypothesis of unit root. Defaults to 0.05.
        - **annot**: (bool) Switch to add p-values to p-value heatmap. Defaults to False.
        - **plt_show**: (bool) Switch to generate p-value heatmap as output. Defaults to True.

    Returns:
        - **df_results**: (pd.DataFrame) Array containing p-values for each variable and lag.
        - **heatmap**: (plot) P-Value heatmap if plt_show = True.
    """
    # Differencing TS for Stationarity Approximation
    for column in df.columns:
        adf_p = adfuller(df[column])[1]
        if adf_p < p_thresh:
            df[column] = df[column].copy().diff(1)
    df.dropna(how='any', inplace=True)

    # Calculate Granger Test p-values
    results = {}
    for column in df.columns:
        if column != target_var:
            test_result = grangercausalitytests(df[[target_var, column]], max_lag, verbose=False)
            p_values = [round(test_result[i + 1][0]['ssr_ftest'][1], 4) for i in range(max_lag)]
            results[column] = p_values
            df_results = pd.DataFrame().from_records(results).T

    # Save in Dataframe
    col_names = [f"lag{i}" for i in range(max_lag)]
    df_results.columns = col_names

    # Create Graph
    if plt_show:
        fig, ax = plt.subplots(figsize=(10, 10))
        sns.heatmap(df_results, annot=annot, cmap='Blues_r', ax=ax)

        plt.title("Granger P-Value Matrix")
        plt.show()

    return df_results


def correlation_matrix(df, method:str='pearson'):
    """
    Plot Correlation matrix for all variables in a dataframe.

    Parameters:
        - **df**: (array-like) Input dataframe containing at least two variables.
        - **method**: (str) Correlation coefficient to be computed. Allows for [pearson, kendall, spearman]. Defaults
                      to pearson.

    Returns:
        - **Correlation matrix**: (array-like) Correlation matrix in tabular format.
        - **Heatmap**: (plot) Heatmap plot of correlation matrix.
    """
    # Calculate Correlation Matrix
    corr = df.corr(method=method)

    # Shape Triangular
    mask = np.triu(np.ones_like(corr))

    # Create Graph
    fig, ax = plt.subplots(figsize=(10,10))
    sns.heatmap(corr, annot=False, cmap='bwr', ax=ax, mask=mask)
    plt.title("Correlation Matrix")
    plt.show()

    return corr