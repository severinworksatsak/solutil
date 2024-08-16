import solutil.evaluations as ev
import solutil.dbqueries as db
from datetime import datetime, timedelta
from sklearn import metrics
from datetime import datetime, timedelta
from pytz import timezone

env_dict = db.get_env_variables(mandant='SAK_ENERGIE', scope='get_ts')

start = datetime.strptime("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
end = datetime.strptime("2024-04-01 00:00:00", "%Y-%m-%d %H:%M:%S")

start21 = datetime.strptime("2021-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
end21 = datetime.strptime("2021-04-01 00:00:00", "%Y-%m-%d %H:%M:%S")

start_local = timezone('Etc/GMT-1').localize(start)
end_local = timezone('Etc/GMT-1').localize(end)
start_str_local = start_local.strftime("%d.%m.%Y %H:%M:%S")
end_str_local = end_local.strftime("%d.%m.%Y %H:%M:%S")

ts = db.get_timeseries_15min(ts_id=3657858, 
                             date_from=start, 
                             date_to=end, 
                             offset_summertime=False,
                             **env_dict)

ts_1h = db.get_timeseries_1h(ts_id=12026798, 
                             date_from=start, 
                             date_to=end, 
                             offset_summertime=True,
                             **env_dict)

ts_1d = db.get_timeseries_1d(ts_id=15779, 
                             date_from=start21, 
                             date_to=end21, 
                             str_table='meanvalues', 
                             **env_dict)

## Test evaluation metrics module

act_inlet1 = 11127586
pred_inlet1 = 11055610
date_from = datetime.strptime('01.03.2021', '%d.%m.%Y')
date_to = datetime.now() - timedelta(days=1)
env_vars = db.get_env_variables(mandant='EPAG_ENERGIE')

y_true = db.get_timeseries_1h(act_inlet1, date_from, date_to, **env_vars)
y_pred = db.get_timeseries_1h(pred_inlet1, date_from, date_to, **env_vars)

ev.get_eval_metrics(y_true, y_pred)
fig = ev.get_act_vs_pred_plot(y_true, y_pred)

## Test ts info retrieval functions
id_list = 10253455 #[10255110, 10253455]

info_data = db.get_ts_info(id_list, **env_dict)


str_name = '%NM%Prognose%'
name_data = db.ts_name_lookup(str_name, **env_dict)








