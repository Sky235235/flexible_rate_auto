import pymysql

class DBConfig:
    def __init__(self, data):

        self.data = data

    def ServiceDev(self):

        ## 서버접근
        # access_data = self.data['service_dev_new_for_server']
        ## pc 접근
        access_data = self.data['service_dev_new']

        host = access_data['host']
        port = access_data['port']
        database = access_data['database']
        username = access_data['username']
        password = access_data['password']

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

    def ServiceStage(self):

        ## 서버접근
        # access_data = self.data['service_stage_for_server']
        ## pc 접근
        access_data = self.data['service_stage']

        host = access_data['host']
        port = access_data['port']
        database = access_data['database']
        username = access_data['username']
        password = access_data['password']

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

    def ServiceRO(self):

        ## 서버접근
        # access_data = self.data['service_live_ro_for_server']
        ## pc 접근
        access_data = self.data['service_live_ro']

        host = access_data['host']
        port = access_data['port']
        database = access_data['database']
        username = access_data['username']
        password = access_data['password']

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

    def ServiceLive(self):
        ## 서버접근 -> PC 없음
        access_data = self.data['service_live_for_server']

        host = access_data['host']
        port = access_data['port']
        database = access_data['database']
        username = access_data['username']
        password = access_data['password']

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

    def CarLogNew(self):
        ## 서버접근
        # access_data = self.data['car_log_ro_for_server']
        ## PC접근
        access_data = self.data['car_log_ro']

        host = access_data['host']
        port = access_data['port']
        database = access_data['database']
        username = access_data['username']
        password = access_data['password']

        conn = pymysql.connect(host=host, user=username, db=database, port=port, password=password)
        curs = conn.cursor(pymysql.cursors.DictCursor)

        return conn, curs

