import json
import redis
import datetime


class RedisAdapter:
    def __init__(self,  host, port, db, password):
        self._redis = redis.Redis(host=host, port=port, db=db, password=password, decode_responses=True)

    def upsert(self, key, value):
        return self._redis.set(key, json.dumps(value, default=self._dump_handler))

    def read(self, key):
        value = self._redis.get(key)
        return json.loads(value, object_hook=self._load_handler)

    def delete(self, key):
        return self._redis.delete(key)

    def keys(self):
        return self._redis.keys()

    def clear_db(self):
        return self._redis.flushdb()

    def exists(self, key):
        return self._redis.exists(key)

    def read_all(self, pattern):
        return self._redis.scan_iter(match=pattern)

    @staticmethod
    def _dump_handler(item):
        if isinstance(item, datetime.datetime):
            return {
                "value": item.timestamp(),
                "__type__": "datetime"
            }
        else:
            return str(item)

    @staticmethod
    def _load_handler(item):
        __type__ = item.get('__type__')
        if not __type__:
            return item

        if __type__ == "datetime":
            return datetime.datetime.utcfromtimestamp(item["value"])
