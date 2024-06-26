import pandas as pd
from sklearn import metrics
import seaborn as sns
import warnings


# Evaluation metrics
def get_eval_metrics(y_true, y_pred):
    """
    Calculate Evaluation Metrics MAE, MSE, RMSE, and MAPE.

    Parameters:
        - **y_true** (pandas.Series): Actual observations of the target variable.
        - **y_pred** (pandas.Series): Predicted values of the target variable.

    Returns:
        - **metric_dict** (dict): Dict containing evaluation metrics as values.
    """
    if len(y_true) != len(y_pred):
        raise warnings.warn('Warning: Length of input series not identical.')

    df_comb = y_true.to_frame(name='y_true')
    df_comb['y_pred'] = y_pred
    df_comb.dropna(inplace=True)

    # Calculate Metrics
    mae = metrics.mean_absolute_error(df_comb['y_true'], df_comb['y_pred'])
    mse = metrics.mean_squared_error(df_comb['y_true'], df_comb['y_pred'])
    rmse = metrics.root_mean_squared_error(df_comb['y_true'], df_comb['y_pred'])
    mape = metrics.mean_absolute_percentage_error(df_comb['y_true'], df_comb['y_pred'])
    nonna = round(len(df_comb) / len(y_true), 2)
    len_true = len(y_true)
    nnas = len_true - len(df_comb)

    # Create Metric Dict
    keys = ['mae', 'mse', 'rmse', 'mape', 'non-na%', 'n_actual_obs', 'n_pred_nas']
    values = [mae, mse, rmse, mape, nonna, len_true, nnas]
    metric_dict = dict(zip(keys, values))

    return metric_dict


# Actual vs. Prediction Plot
def get_act_vs_pred_plot(y_true, y_pred):
    """
    Create actual vs. prediction line plot.

    Parameters:
        - **y_true** (pandas.Series): Actual observations of the target variable.
        - **y_pred** (pandas.Series): Predicted values of the target variable.

    Returns:
        - **fig** (matplotlib.figure.Figure): Figure object containing axes filled with line plot.
    """
    # Merge series
    y_true.name = 'actual'
    df_data = y_true.to_frame()
    df_data['predicted'] = y_pred
    df_data['time'] = df_data.index

    # Melt dataframe
    df_melt = pd.melt(df_data, id_vars='time')

    # Create figure
    fig = sns.lineplot(data=df_melt, x='time', y='value', hue='variable', palette=['#660066', '#FFEB00'])

    return fig
