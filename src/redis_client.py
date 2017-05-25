import redis
from conf  import REDIS_HOST,REDIS_PORT #REDIS_PORT,REDIS_PASSWD,REDIS_PORT,REDIS_USER

class RedisClient:

    def __init__(self):
        self.host = REDIS_HOST
        self.port = REDIS_PORT

        self.client = redis.StrictRedis(host=self.host, port=self.port)
        self.pipe = self.client.pipeline()

    def set_key(self, k,v):
        return self.client.set(k,v)

    def get_value(self,k):

        return self.client.get(k)

    def store_dataset(self, data_dict):

        pipe = self.pipe
        for (k, v) in data_dict.iteritems():
            pipe.hmset(k, v)

    def execute(self):
        self.pipe.execute()

    def load_data_into_redis(self,dict):

        pipe = self.pipe
        for i, (k, v) in enumerate(dict.iteritems()):
            pipe.hmset(k, v)

        pipe.execute()

    def get_dbsize(self):
        return self.client.dbsize()





