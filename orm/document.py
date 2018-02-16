import abc
import copy

from abc import abstractmethod

from .fields import BaseField
from _adapters import RedisAdapter, FirebaseAdapter

from orm import get_connections


class _Flag:
    def __init__(self):
        self._value = False

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = bool(value)


class DocumentMetaclass(abc.ABCMeta):
    def __new__(mcs, name, bases, attrs):
        conn = attrs.get('_connection')
        if isinstance(conn, dict) and {'redis', 'firebase'} <= conn.keys():
            redis_props = conn['redis']
            firebase_props = conn['firebase']
            attrs['_container'] = conn['firebase'].pop('container', '')

            attrs['_redis'] = RedisAdapter(**redis_props)
            attrs['_firebase'] = FirebaseAdapter(**firebase_props)

        fields = {}
        fieldnames = [
            name for name, item in attrs.items()
            if isinstance(item, BaseField)
        ]

        for fieldname in fieldnames:
            fields[fieldname] = attrs[fieldname]
            del attrs[fieldname]

        attrs['__fields__'] = fields

        return super().__new__(mcs, name, bases, attrs)


class Document(metaclass=DocumentMetaclass):
    def __init__(self, *, ignore_non_existing=False, **kwargs):
        fields = copy.deepcopy(self.__fields__)

        if not ({'_redis', '_firebase'} <= self.__class__.__dict__.keys()):
            redis, firebase = get_connections()
            if not redis and not firebase:
                raise ConnectionError('Connections to Firebase and Redis not found')

            object.__setattr__(self, '_redis', redis)
            object.__setattr__(self, '_firebase', firebase)

        document_id = None
        for name, field in fields.items():
            if name in kwargs:
                field.value = kwargs[name]

            if field.is_id:
                document_id = field.value

        if not document_id:
            raise AttributeError("Document must have id field!")

        if self._redis.exists(document_id):
            entity = self._redis.read(document_id)
            for key, value in entity.items():
                if key in fields:
                    fields[key].value = value
                elif not ignore_non_existing:
                    raise AttributeError("There is no such field as {}".format(key))

        object.__setattr__(self, '_changed', _Flag())
        object.__setattr__(self, '_id', document_id)
        object.__setattr__(self, '_fields', fields)
        object.__setattr__(self, '_path', "{}/{}".format(self._container, self._id))

    def __setattr__(self, key, value):
        if key in self._fields:
            self._changed.value = True
            self._fields[key].value = value
            return value

        raise AttributeError("There is no such field: {}".format(key))

    def __getattr__(self, key):
        if key in self._fields:
            return self._fields[key].value
        
        raise AttributeError("There is no such field: {}".format(key))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save()

    def save(self, force=False):
        if not force and not self._changed.value:
            return

        document_view = DocumentView(self)

        if callable(self.presentation):
            presentation = self.presentation(document_view)
        else:
            presentation = document_view

        self._changed.value = False
        self._redis.upsert(self._id, document_view)
        self._firebase.update(self._path, presentation)

        if callable(self.on_save):
            self.on_save(document_view)

    def delete(self):
        self._redis.delete(self._id)
        self._firebase.delete(self._path)

    on_save = None
    presentation = None


class DocumentView(dict):
    def __init__(self, document: Document):
        super().__init__()

        self.update({
            key: item.value
            for key, item in document._fields.items()
        })

    def __getattr__(self, item):
        if item in self:
            return self[item]

        raise AttributeError("No such attribute: {}!".format(item))


class PresentationDocument(Document, metaclass=abc.ABCMeta):
    def save(self, force=False):
        if not force and not self._changed.value:
            return

        document_view = DocumentView(self)
        presentation = self.presentation(document_view)
        self._firebase.update(self._path, presentation)

    @staticmethod
    @abstractmethod
    def presentation(document: DocumentView):
        pass
