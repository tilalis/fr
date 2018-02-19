import functools
import firebase_admin

from threading import Thread

from firebase_admin.db import ApiCallError
from firebase_admin import credentials, db, storage


class FirebaseAdapter:
    def __init__(self, database, bucket, cred):
        cred = credentials.Certificate(cred)

        # Initialize the app with a service account, granting admin privileges
        firebase_admin.initialize_app(cred, {
            'databaseURL': database,
            'storageBucket': bucket
        })

        self._bucket = storage.bucket()

    @staticmethod
    def read(path):
        return db.reference(
            path=path
        ).get()

    @staticmethod
    def create(path, key, value):
        FirebaseAdapter._execute(
            lambda: db.reference(
                path=FirebaseAdapter._path(path, key)
            ).set(value)
        )

    @staticmethod
    def update(path, key, value: dict):
        no_none = {
            k: v
            for k, v in value.items()
            if v is not None
        }

        if no_none:
            FirebaseAdapter._execute(
                lambda: db.reference(
                    path=FirebaseAdapter._path(path, key)
                ).update(no_none)
            )

    @staticmethod
    def delete(path, key):
        FirebaseAdapter._execute(
            lambda: db.reference(path=FirebaseAdapter._path(path, key)).delete()
        )
        
    @staticmethod
    def _execute(action, *args, **kwargs):
        Thread(target=functools.partial(action, *args, **kwargs)).start()

    @staticmethod
    def _path(path, key):
        return "{}{}".format(path, key)