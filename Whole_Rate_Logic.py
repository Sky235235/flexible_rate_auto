import pandas as pd
import json
from datetime import timedelta, datetime
from tqdm import tqdm
##Module
from edamodule.QueryConfig import ServiceQuery
from edamodule.DBConfig import DBConfig
from edamodule.InsertLoadModule import DataLoad, InsertDB
from edamodule.SectorUtils import GetCoordinate

print('Setting Datetime')
freq = '10min'
start_day = datetime.now() - timedelta(seconds=600)
datetime_idx = pd.DatetimeIndex([start_day]).floor(freq=freq)
start_datetime = str(datetime_idx[0])

end_datetime_idx = datetime_idx + timedelta(seconds=599)
end_datetime = str(end_datetime_idx[0])

today_now = datetime.now()
now_datetime_idx = pd.DatetimeIndex([today_now]).floor(freq=freq)
now_datetime_group = str(now_datetime_idx[0])

# ## 테스트
# start_datetime = '2023-04-06 13:00:00'
# end_datetime = '2023-04-06 13:09:59'
# now_datetime_group = '2023-04-06 13:10:00'

print(start_datetime, end_datetime, now_datetime_group)

print('Load Service Data')
## Load configure file
with open('edamodule/dbconfiginfo.json', 'r') as f:
    config_data = json.load(f)
## query setting
config_query = ServiceQuery()
service_query = config_query.Get_Boarding_history(start_datetime, end_datetime)
## db configuration
dbconfig = DBConfig(config_data)
conn, curs = dbconfig.ServiceRO()
## Load configuration
loadconfig = DataLoad(conn, curs)
## load data
service_df = loadconfig.get_data(service_query)
del loadconfig, config_query, dbconfig

print('Extract call_count and dispatch_count group by Sector')

if service_df.empty:
    total_service_agg = pd.DataFrame({'datetime_group': [now_datetime_group],
                                      'call_count_ago': [1],
                                      'no_dispatch_count_ago': [1]})
else:
    service_df.loc[service_df['boarding_datetime'] == start_datetime, 'boarding_datetime'] = datetime_idx[0] + timedelta(seconds=1) ## LIVE 시
    # service_df.loc[service_df['boarding_datetime'] == start_datetime, 'boarding_datetime'] = '2023-04-06 13:10:01'  ## 테스트시
    service_df['datetime_group'] = service_df['boarding_datetime'].dt.ceil(freq=freq)
    no_dispatch_df = service_df[service_df['status'].isin([1, 2, 5])].reset_index()
    service_agg = service_df.groupby(['datetime_group'])['dispatch_idx'].count().reset_index()
    service_agg = service_agg.rename(columns=dict(dispatch_idx='call_count_ago'))
    no_dispatch_agg = no_dispatch_df.groupby(['datetime_group'])['dispatch_idx'].count().reset_index()
    no_dispatch_agg = no_dispatch_agg.rename(columns=dict(dispatch_idx='no_dispatch_count_ago'))
    total_service_agg = pd.merge(service_agg, no_dispatch_agg, how='left', on='datetime_group')
    total_service_agg['call_count_ago'] = total_service_agg['call_count_ago'].fillna(1)
    total_service_agg['no_dispatch_count_ago'] = total_service_agg['no_dispatch_count_ago'].fillna(0)
del service_agg, no_dispatch_agg


total_service_agg['datetime_group'] = pd.to_datetime(total_service_agg['datetime_group'])
total_service_agg['dispatch_count_ago'] = total_service_agg['call_count_ago'] - total_service_agg['no_dispatch_count_ago']
total_service_agg['dispatch_rate'] = round(total_service_agg['dispatch_count_ago'] / total_service_agg['call_count_ago'], 2)

print('Define Holiday YN')
total_service_agg['weekday'] = total_service_agg['datetime_group'].dt.weekday
total_service_agg['date'] = total_service_agg['datetime_group'].dt.date
total_service_agg['date'] = pd.to_datetime(total_service_agg['date'])
total_service_agg['time'] = total_service_agg['datetime_group'].dt.time
total_service_agg['hour'] = total_service_agg['datetime_group'].dt.hour
## Merge With holiday_info
holiday_info = pd.read_csv('edamodule/holiday_info.csv', index_col=0)
holiday_info['date'] = pd.to_datetime(holiday_info['date'])
_holiday_info_merge = holiday_info[['date', 'holiday']]
del holiday_info
total_service_agg = pd.merge(total_service_agg, _holiday_info_merge, how='left', on='date')
total_service_agg.loc[total_service_agg['weekday'].isin([0, 1, 2, 3, 4]), 'holiday_yn'] = 0
total_service_agg.loc[total_service_agg['weekday'].isin([5, 6]), 'holiday_yn'] = 1
total_service_agg.loc[total_service_agg['holiday'] == 1, 'holiday_yn'] = 1

print('Get Basic Rate')
from edamodule.RateUtils import GetFlexibleRate
total_service_agg['integrated_group'] = 1
## DB configuration
_dbconfig = DBConfig(config_data)
_conn, _curs = _dbconfig.ServiceRO()
_loadconfig = DataLoad(_conn, _curs)
## Get_Rate Function Configuration
#### get_basic_rate
get_rate = GetFlexibleRate(_loadconfig)
basic_rate_lst = get_rate.get_basic_rate(total_service_agg)
### get_time_min_max_rate
time_min_lst = get_rate.get_time_min_max_rate(total_service_agg)
total_service_agg['basic_flexible_rate'] = basic_rate_lst
total_service_agg['time_min_rate'] = time_min_lst
total_service_agg[['basic_flexible_rate', 'time_min_rate']] = total_service_agg[['basic_flexible_rate', 'time_min_rate']].astype('float')

print('Replace time_min_rate if basic_rate less than time_min_rate')
modify_rate_lst = []
for i in tqdm(range(len(total_service_agg))):
    _basic_rate = total_service_agg['basic_flexible_rate'][i]
    _time_min_rate = total_service_agg['time_min_rate'][i]
    if _basic_rate < _time_min_rate:
        _modify_rate = _time_min_rate
    else:
        _modify_rate = _basic_rate
    modify_rate_lst.append(_modify_rate)
total_service_agg['basic_flexible_rate_modify'] = modify_rate_lst

print('Get Precipitation Rate')
## Merge With Weather
from edamodule.REDIS import ConnectRedis
redis = ConnectRedis()
redis_key = 'weather_df'
weather_df = redis.load_df_from_redis(redis_key)
weather_df['date'] = pd.to_datetime(weather_df['date'])
total_service_agg = pd.merge(total_service_agg, weather_df, how='left', on=['date', 'hour'])
## When weather_df is empty
if total_service_agg['rain_type'].isnull().sum() > 0:
    print('Merge With short Weather DataFrame')
    total_service_agg = total_service_agg.drop(['rain_type', 'rainfall'], axis=1)
    redis_key = 'short_weather_df'
    weather_df = redis.load_df_from_redis(redis_key)
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    total_service_agg = pd.merge(total_service_agg, weather_df, how='left', on=['date', 'hour'])

precipitation_rate_lst = get_rate.get_precipitation_rate(total_service_agg)
total_service_agg['precipitation_rate'] = precipitation_rate_lst
del _loadconfig, _conn, _curs

print('White and Hiblack')
_total_service_agg_white = total_service_agg.copy()
_total_service_agg_white['car_type_idx'] = 1
_total_service_agg_hiblack = total_service_agg.copy()
_total_service_agg_hiblack['car_type_idx'] = 3

print('Hiblack and White Night Fare')
## Night Fare is not night premium which is 22 hour to next day 4 hour added rate
## Night Fare is add or subtract more rate at night
_total_service_agg_white['night_fare'] = 0
_night_fare_lst = get_rate.get_night_fare(_total_service_agg_hiblack)
_total_service_agg_hiblack['night_fare'] = _night_fare_lst
total_integrated_rate = pd.concat([_total_service_agg_white, _total_service_agg_hiblack]).reset_index(drop=True)
del total_service_agg, _total_service_agg_white, _total_service_agg_hiblack, get_rate

print('Get Flexible_rate_auto_yn')
## DB config
dbconfig = DBConfig(config_data)
conn, curs = dbconfig.ServiceRO()
## Query config
config_query = ServiceQuery()
auto_yn_query = config_query.Get_flexible_rate_auto_yn()
## Load config
loadconfig = DataLoad(conn, curs)
auto_yn_df = loadconfig.get_data(auto_yn_query)
auto_yn_value = auto_yn_df['flexible_fare_rate_auto_yn'][0]
total_integrated_rate['flexible_fare_rate_auto_yn'] = auto_yn_value
del dbconfig, config_query, loadconfig
print('Insert DB')
## DB config
dbconfig = DBConfig(config_data)
conn, curs = dbconfig.ServiceLive()
# conn, curs = dbconfig.ServiceDev()
insert_config = InsertDB(conn, curs)
insert_config.insert_auto_rate_table(total_integrated_rate)

print(total_integrated_rate)
# total_integrated_rate.to_csv('test.csv', encoding='utf-8-sig')

