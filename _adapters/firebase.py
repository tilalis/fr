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
        return db.reference(path=path).get()

    @staticmethod
    def create(path, value):
        FirebaseAdapter._execute(
            lambda: db.reference(path=path).set(value)
        )

    @staticmethod
    def update(path, value: dict):
        FirebaseAdapter._execute(
            lambda: db.reference(path=path).update({
                k: v
                for k, v in value.items() if value is not None
            })
        )

    @staticmethod
    def delete(path):
        FirebaseAdapter._execute(
            lambda: db.reference(path=path).delete()
        )
        
    @staticmethod
    def _execute(action, *args, **kwargs):
        Thread(target=functools.partial(action, *args, **kwargs)).start()
