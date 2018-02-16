from _adapters import RedisAdapter, FirebaseAdapter

_connections = {}


def connect(redis: dict, firebase: dict):
    _connections['redis'] = RedisAdapter(**redis)
    _connections['firebase'] = FirebaseAdapter(**firebase)


def get_connections():
    return _connections['redis'], _connections['firebase']
