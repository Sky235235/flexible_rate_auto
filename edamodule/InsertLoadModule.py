import pymysql
import pandas as pd
from tqdm import tqdm

class DataLoad:
    def __init__(self, conn, curs):
        self.conn = conn
        self.curs = curs

    def get_data(self, query):
        self.curs.execute(query)
        data = pd.DataFrame(self.curs.fetchall())
        return data

    def __del__(self):
        self.curs.close()
        self.conn.close()

class InsertDB:
    def __init__(self, conn, curs):
        self.conn = conn
        self.curs = curs

    def __del__(self):
        self.curs.close()
        self.conn.close()

    def insert_auto_rate_table(self, data):
        query = """****"""

        for i in tqdm(range(len(data))):
            date_timegroup = data['datetime_group'][i]
            car_type_idx = data['car_type_idx'][i]
            flexible_rate = data['basic_flexible_rate_modify'][i]
            dispatch_rate = data['dispatch_rate'][i]
            call_count = data['call_count_ago'][i]
            dispatch_count = data['dispatch_count_ago'][i]
            flexible_rate_call = data['manual_flexible_rate'][i]
            night_fare = data['night_fare'][i]
            additional_rate = data['additional_rate'][i]
            precipitation_rate = data['precipitation_rate'][i]
            integrated_group = data['integrated_group'][i]
            flexible_fare_rate_auto_yn = data['flexible_fare_rate_auto_yn'][i]

            vals = (date_timegroup, car_type_idx, flexible_rate, dispatch_rate, call_count,
                    dispatch_count, flexible_rate_call, night_fare, additional_rate,
                    precipitation_rate, integrated_group, flexible_fare_rate_auto_yn)
            self.curs.execute(query, vals)
            self.conn.commit()

    def insert_cluster_auto_rate_table(self, data):
        ''' 현재 stage 에만 적재중'''
        query = """*****"""

        for i in tqdm(range(len(data))):
            date_timegroup = data['datetime_group'][i]
            car_type_idx = data['car_type_idx'][i]
            flexible_rate = data['basic_flexible_rate_modify'][i]
            dispatch_rate = data['dispatch_rate'][i]
            call_count = data['call_count_ago'][i]
            dispatch_count = data['dispatch_count_ago'][i]
            flexible_rate_call = data['manual_flexible_rate'][i]
            night_fare = data['night_fare'][i]
            additional_rate = data['additional_rate'][i]
            precipitation_rate = data['precipitation_rate'][i]
            integrated_group = data['integrated_group'][i]
            flexible_fare_rate_auto_yn = data['flexible_fare_rate_auto_yn'][i],
            cluster = data['cluster'][i],
            time_additional_rate = data['time_additional_rate'][i]

            vals = (date_timegroup, car_type_idx, flexible_rate, dispatch_rate, call_count,
                    dispatch_count, flexible_rate_call, night_fare, additional_rate,
                    precipitation_rate, integrated_group, flexible_fare_rate_auto_yn,
                    cluster, time_additional_rate)
            self.curs.execute(query, vals)
            self.conn.commit()



