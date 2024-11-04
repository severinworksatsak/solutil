from datetime import datetime, timedelta
from sklearn.linear_model import LinearRegression, Ridge
import pandas as pd
import numpy as np
import json
from pathlib import Path
import holidays
from solutil.dbqueries import get_timeseries_15min, get_env_variables


class Forecaster:

    def __init__(self):
        # Define config
        self.forecast = {
            "max_window_size": 31,
            "window_size": 10,
            "expand_step": 5,
            "n_val_min": 1,
            "n_iter_max": 4,
            "daytype_replace": 0,
            "tz": "Europe/Zurich",
            "freq": "h"
        }

    def locate_in_array(self, input_array, validation_idx):
        """
        Filter input array by index location.

            - **input_array**: Float input array being compared against the index locators.
            - **validation_idx**: Input array containing bool indicators for the locations in an index to be selected.

        Returns:
            - **output_array**: (np.array) Float array of values corresponding to the True index locations.
        """
        output_array = np.zeros(sum(validation_idx))
        i_out = 0
        for i, element in enumerate(input_array):
            if validation_idx[i]:
                output_array[i_out] = element
                i_out += 1

        return output_array

    def locate_in_int_array(self, input_array, validation_idx):
        """
        Filter input array by index location.

        Parameters:
            - **input_array**: Integer input array being compared against the index locators.
            - **validation_idx**: Input array containing bool indicators for the locations in an index to be selected.

        Returns:
            - Integer array of values corresponding to the True index locations.
        """
        output_array = np.zeros(sum(validation_idx), dtype=np.int64)
        i_out = 0
        for i, element in enumerate(input_array):
            if validation_idx[i]:
                output_array[i_out] = element
                i_out += 1

        return output_array

    def create_prog_strawman(self, date_from, date_to):
        """
        Create strawman with datetime index, filled with dummy data to be replaced in subsequent methods.

            - **date_from**: (datetime) Forecast start period.
            - **date_to**: (datetime) Forecast end period.

        Returns:
            -  **prog_data**: (pd.Series) Data structure strawman with np.zeros vector.
        """
        date_range = pd.date_range(start=date_from, end=date_to, inclusive='left', freq='1h', tz='Europe/Zurich')
        dummy_data = np.zeros(len(date_range))
        prog_data = pd.Series(data=dummy_data, index=date_range)

        return prog_data

    def get_forecast_inputs(self, data_input:pd.Series):
        """
        Prepare vtv forecast by computing the required variables for the vtv method.

        Parameters:
            - **data_input**: (pd.Series) with datetime index containing load of either base year or empty forecast
                              strawman. Frequency can be either D, h or 15min. If 15min, series will be resampled to 1h.

        Returns:
            - DataFrame containing all required variables for vtv algorithm
        """
        # Load parameters from config if not specified
        default_params = self.forecast.copy()

        # used_tz = params_dict['tz']                         # timezone to be used
        used_freq = pd.infer_freq(data_input.index)  # used frequency for date ranges
        if used_freq == '15min':
            data_input = data_input.resample('1h').mean() # Also converts kW to kWh

        # Create sin-cos-transformation-like date encoding for continuous time representation
        index = data_input.index.copy().tz_localize(None)
        index_offset = index - timedelta(days=91)

        date_enc1 = [abs((time - datetime(time.year - 1, 12, 31)).days - 183) for time in index]
        date_enc2 = [abs((time - datetime(time.year - 1, 12, 31)).days - 183) for time in index_offset]

        # Create day types for historical & forecast data
        holiday_sg = holidays.CH(prov='SG')

        daytype_array = data_input.index.weekday.array
        weekday_array = daytype_array.copy()
        holiday_array = pd.array([date_in in holiday_sg for date_in in data_input.index.date])
        preholiday_array = pd.array([date_in in holiday_sg for date_in in (data_input.index.date - timedelta(days=1))])
        postholiday_array = pd.array([date_in in holiday_sg for date_in in (data_input.index.date + timedelta(days=1))])

        daytype_array[holiday_array] = 16
        daytype_array[preholiday_array & (weekday_array == 0)] = 10  # Montag vor Feiertag
        daytype_array[postholiday_array & (weekday_array == 4)] = 14  # Freitag nach Feiertag

        # Get Hours and Summertime offsets
        hours = data_input.index.hour
        minutes = data_input.index.minute

        index_offset = data_input.index.tz_convert('CET')  # + timedelta(days=1)
        summertime_offset = [index_offset[i].tzinfo._dst.seconds / 3600 for i in range(len(data_input))]

        # Create date_helper
        year_help = data_input.index.year
        month_help = data_input.index.month
        day_help = data_input.index.day
        date_helper = year_help * 10000 + month_help * 100 + day_help
        date_helper_float = date_helper + hours / 25  # / 25 to ensure [0, 1]

        # Create combined master dataframe
        df_combined = pd.DataFrame({
            'data': data_input,
            'date_enc1': date_enc1,
            'date_enc2': date_enc2,
            'daytype': daytype_array,
            'summertime_offset': summertime_offset,
            'date_helper': date_helper,
            'date_helper_float': date_helper_float
        }, index=data_input.index)

        if used_freq in ('h', '15min', None):
            df_combined['hours'] = hours
            df_combined['minutes'] = minutes

        return df_combined

    def evaluate_forecast(self, df_hist, df_forecast):
        """
        Evaluate each year of forecast by benchmarking parameters to reference lastgang.

        Parameters:
            - **df_hist**: (pd.Series) Historical reference lastgang.
            - **df_forecast**: (pd.Series) (Multi-year) VTV forecast based on historical lastgang.

        Returns:
            - **output**: (dict) Yearly comparison of forecast with base lastgang.
        """

        freq_hist = (df_hist.index[1] - df_hist.index[0]).seconds / 3600
        freq_prog = (df_forecast.index[1] - df_forecast.index[0]).seconds / 3600

        # Calculate historic MWh / kWh
        hist_work = df_hist['data'].sum() / freq_hist

        # Calculate forecasted MWh / kWh per year
        year_dict = {}
        for i_year in df_forecast.index.year.unique():
            prog_sliced = df_forecast.loc[df_forecast.index.year == i_year]
            prog_work = prog_sliced['forecast'].sum() / freq_prog
            year_dict[i_year] = prog_work

        output = {
            'hist_work': hist_work,
            'prog_work': year_dict,
        }

        return output

    def get_vtv_forecast(self, df_hist, df_prog, **kwargs):
        """
        Roll out Lastgang series by applying VTV procedure on reference year Lastgang.

        Parameters:
            - **df_hist**: Dataframe containing historical load data and prepared inputs from get_forecast_inputs.
            - **df_prog**: DataFrame containing to-be-filled series for future periods and prepared inputs from get_forecast_inputs.
            - **kwargs**: Possible kwargs: [max_window_size, window_size, expand_step, v_val_min, tz, freq]

        Returns:
            - df_out (pd.Series): VTV forecast output (in same unit as input) with datetime index.
        """
        print("Forecast started.")

        # Load parameters from config if not specified
        default_params = self.forecast.copy()
        params_dict = {param: kwargs.get(param, default_params[param]) for param in default_params}

        # max_window_size = params_dict['max_window_size']  # max compare window size
        config_window_size = params_dict['window_size']  # initial window size
        config_dayshift = params_dict['daytype_replace']  # day int value for
        expand_step = params_dict['expand_step']  # step size for compare window expansion
        n_val_min = params_dict['n_val_min']  # minimum validated values in compare window
        n_iter_max = params_dict['n_iter_max']  # maximum window size expansions
        used_freq = pd.infer_freq(df_prog.index)  # Frequency of the to-be-predicted data strawman index

        # Convert hist df to numpy elements
        hist_data = df_hist['data'].to_numpy()
        hist_date_enc1 = df_hist['date_enc1'].to_numpy()
        hist_date_enc2 = df_hist['date_enc2'].to_numpy()
        hist_daytype = df_hist['daytype'].to_numpy()
        hist_dst_offset = df_hist['summertime_offset'].to_numpy()
        hist_datehelper = df_hist['date_helper'].to_numpy()
        if used_freq in ('h', '15min'):
            hist_hours = df_hist['hours'].to_numpy()

        # Convert prog df to numpy elements
        prog_data = df_prog['data'].copy().to_numpy()
        prog_date_enc1 = df_prog['date_enc1'].to_numpy()
        prog_date_enc2 = df_prog['date_enc2'].to_numpy()
        prog_daytype = df_prog['daytype'].to_numpy()
        prog_dst_offset = df_prog['summertime_offset'].to_numpy()
        prog_datehelper = df_prog['date_helper'].to_numpy()
        prog_datehelper_float = df_prog['date_helper_float'].to_numpy()
        if used_freq in ('h', '15min'):
            prog_hours = df_prog['hours'].to_numpy()

        # Get unique days from datehelper & corresponding prog values
        unique_index = (np.mod(prog_datehelper_float, 1) == 0)  # Get first hour for each day
        prog_datehelper_unique = prog_datehelper[unique_index]
        enc1_unique = prog_date_enc1[unique_index]
        enc2_unique = prog_date_enc2[unique_index]
        daytype_unique = prog_daytype[unique_index]

        # Create assignment variables
        prog_nval = np.zeros(prog_data.size)
        prog_niter = np.zeros(prog_data.size)
        prog_window_size = np.zeros(prog_data.size)

        n_forecast = 0

        # Loop over unique forecast days
        for i_day, date_day in enumerate(prog_datehelper_unique):  # prog_datehelper_unique > 20240301 format
            n_days = 0
            n_iter = 0
            n_expand_iter = 0
            window_size = config_window_size
            dayshift = config_dayshift

            # Ensure minimum day values in sample
            while (n_days < n_val_min) and (n_iter < n_iter_max):
                day_index = (
                        (hist_daytype == daytype_unique[i_day] - dayshift) &
                        (hist_date_enc1 <= enc1_unique[i_day] + window_size) &
                        (hist_date_enc1 >= enc1_unique[i_day] - window_size) &
                        (hist_date_enc2 <= enc2_unique[i_day] + window_size) &
                        (hist_date_enc2 >= enc2_unique[i_day] - window_size)
                )
                n_days = sum(day_index)
                n_iter += 1
                window_size += expand_step
                n_expand_iter += 1

                # Replace daytype with alternative daytype
                if n_iter >= n_iter_max:
                    if (daytype_unique[i_day] in [10, 14, 16]) and (dayshift == 0):
                        window_size = 10
                        dayshift = 10
                        n_iter = 0

            # Filter hours matching above criteria
            prog_hours_sel = prog_hours[prog_datehelper == date_day] # one day
            prog_dst_offset_sel = prog_dst_offset[(prog_datehelper == date_day) & (prog_hours == 3)] # one offset value -> +1 or 0
            hist_hours_sel = hist_hours[day_index] # several days according to window size
            hist_dst_offset_sel = hist_dst_offset[day_index]
            hist_data_sel = hist_data[day_index]

            # Compute dst-adjusted hour array
            offset_diff = hist_dst_offset_sel - prog_dst_offset_sel
            hist_hours_dst_adj = hist_hours_sel - offset_diff
            hist_hours_dst_adj = np.where(hist_hours_dst_adj<0, 23, hist_hours_dst_adj) # Eliminate -1 value from subtracting +1 dst offset from hour

            # Loop over all hours of the forecast day
            for i_hour in prog_hours_sel:
                # Filter hours for selected interval & summertime offset
                hist_data_h_sel = hist_data_sel[hist_hours_dst_adj == i_hour]

                # Calculate Mean Forecast from Reference data
                n_hour_val = hist_data_h_sel.size
                sum_hour_val = sum(hist_data_h_sel)
                mean_hour_val = sum_hour_val / n_hour_val if n_hour_val > 0 else 0

                # Assign forecast to array
                prog_data[n_forecast] = mean_hour_val
                prog_nval[n_forecast] = n_hour_val
                prog_niter[n_forecast] = n_expand_iter

                n_forecast += 1

        df_out = pd.DataFrame({
            'forecast': prog_data,
            'nval': prog_nval,
            'n_iter': prog_niter
        }, index=df_prog.index)

        print("Forecast complete.")

        return df_out


    # VTV Lastgang rollout
    def rollout_lastgang(self, prog_start, prog_end, mandant:str='SAK_ENERGIE', df_hist:pd.DataFrame=None, belvis_dict:dict=None):
        """
        Roll out lastgang based on 15min data.

        Parameters:
            - **prog_start**: (datetime) Start date of prediction window. Should be datetime object.
            - **prog_end**: (datetime) End date of prediction window. Should be datetime object.
            - **mandant**: (str) Name of Belvis mandant from which timeseries is coming. Will be used to load environment vars
                        mandant_user, mandant_pwd and mandant_addr.
            - **df_hist**: (pd.Series) Historical lastgang series with datetime index to be rolled out. If none is provided,
                        time series will be loaded from Belvis directly.
            - **belvis_dict**: (dict) Dictionary containing parameters required for get_timeseries_15min, more specifically:
                           ts_id: (int) | date_from (datetime) | date_to (datetime)

        Returns:
        - **tuple of (pd.DataFrame)**: Historical data (0) and forecast dataframe (1)
        """
        # Load timeseries
        env_vars = get_env_variables(mandant=mandant)
        if df_hist is None:
            df_hist = get_timeseries_15min(**belvis_dict | env_vars) # Unpack two dicts at once

        # Prepare forecast inputs

        input_hist = self.get_forecast_inputs(df_hist)
        df_prog = self.create_prog_strawman(date_from=prog_start, date_to=prog_end)
        input_prog = self.get_forecast_inputs(df_prog)

        # Roll out Lastgang
        output_prog = self.get_vtv_forecast(df_hist=input_hist, df_prog=input_prog)

        return df_hist, output_prog