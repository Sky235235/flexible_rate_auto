import redis
import io
import pandas as pd

class ConnectRedis:
    def __init__(self):
        # Local _redis
        host = '***'
        port = '*****'
        db = 0
        max_connections = 4

        redis_pool = redis.ConnectionPool(host=host, port=port, db=db, max_connections=max_connections)
        self.r = redis.Redis(connection_pool=redis_pool)

    def get_keys(self, key):
        ''' key로 redis에 저장되어 있는지 확인'''
        r = self.r
        return r.keys(key)

    def store_df_in_redis(self, key, df, expired):
        '''key로 redis 저장할 key 저장
        df : 저장할 DataFrame,
        expired : redis 저장 만료 시간, 
        DataFrame 직렬화를 위해 io.BytesIO 사용'''
        
        r = self.r
        buffer = io.BytesIO()
        df.to_parquet(buffer, compression='gzip')
        buffer.seek(0)
        r.set(key, buffer.read())
        if expired:
            r.expire(key, expired)

    def load_df_from_redis(self, key):
        r = self.r
        buffer = io.BytesIO(r.get(key))
        buffer.seek(0)
        df = pd.read_parquet(buffer)
        return df

