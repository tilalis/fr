from _adapters import RedisAdapter, FirebaseAdapter

_connections = {
    'redis': None,
    'firebase': None
}

_connection_adapters = {
    'redis': RedisAdapter,
    'firebase': FirebaseAdapter
}


def adapters(redis, firebase):
    _connection_adapters['redis'] = redis
    _connection_adapters['firebase'] = firebase


def connect(redis: dict, firebase: dict):
    redis_adapter, firebase_adapter = _connection_adapters['redis'], _connection_adapters['firebase']

    _connections['redis'] = redis_adapter(**redis)
    _connections['firebase'] = firebase_adapter(**firebase)


def connections():
    return _connections['redis'], _connections['firebase']
