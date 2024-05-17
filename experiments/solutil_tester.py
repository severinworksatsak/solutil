import dbqueries as db
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

ts = db.get_timeseries_15min(ts_id=12026798, 
                             date_from=start, 
                             date_to=end, 
                             offset_summertime=True,
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

