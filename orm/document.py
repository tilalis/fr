import abc
import copy

from abc import abstractmethod

from .fields import BaseField
from _adapters import RedisAdapter, FirebaseAdapter

from orm import get_connections


class DocumentMetaclass(abc.ABCMeta):
    def __new__(mcs, name, bases, attrs):
        conn = attrs.get('_connection')
        if isinstance(conn, dict) and {'redis', 'firebase'} <= conn.keys():
            redis_props = conn['redis']
            firebase_props = conn.get('firebase', {})
            attrs['_container'] = firebase_props.pop('container', '')

            attrs.update({
                '_redis': RedisAdapter(**redis_props) if redis_props else None,
                '_firebase': FirebaseAdapter(**firebase_props) if firebase_props else None
            })

        fields = {}
        fieldnames = [
            name for name, item in attrs.items()
            if isinstance(item, BaseField)
        ]

        presentational = set()

        for fieldname in fieldnames:
            field = attrs[fieldname]
            fields[fieldname] = field

            if field.presentational:
                presentational.add(fieldname)

            del attrs[fieldname]

        attrs['__fields__'] = fields
        attrs['_presentational'] = presentational

        return super().__new__(mcs, name, bases, attrs)


class Document(metaclass=DocumentMetaclass):
    def __new__(cls, *args, **kwargs):
        document = super().__new__(cls)

        fields = copy.deepcopy(cls.__fields__)

        if not ({'_redis', '_firebase'} <= cls.__dict__.keys()):
            redis, firebase = get_connections()
            if not redis and not firebase:
                raise ConnectionError('Connections to Firebase and Redis not found')

            # TODO: Make them class-level instead of instance-level (Maybe move to metaclass)
            object.__setattr__(document, '_redis', redis)
            object.__setattr__(document, '_firebase', firebase)

        document_id = None
        for name, field in fields.items():
            if name in kwargs:
                field.value = kwargs[name]

            if not document_id and field.is_id:
                document_id = field.value

        object.__setattr__(document, '_id', document_id)
        object.__setattr__(document, '_fields', fields)

        return document

    def __init__(self, ignore_non_existing=False, override=False, **kwargs):
        self._ignore_non_existing = ignore_non_existing
        self._path = "{}/".format(self._container)
        self._fetched = override
        self._changed = set()

    def get(self, id):
        # TODO: Make this method static

        if id is None:
            raise AttributeError("Document must have id field!")

        if self._redis.exists(id):
            object.__setattr__(self, '_id', id)
            return self._fetch()

        raise LookupError('No such document with id: {}'.format(id))

    def _fetch(self):
        entity = self._redis.read(self._id)
        for key, value in entity.items():
            if key in self._fields:
                self._fields[key].value = value
            elif not self._ignore_non_existing:
                raise AttributeError("There is no such field as {}".format(key))

        self._fetched = True
        return self

    def __setattr__(self, key, value):
        if key in self._fields:
            field = self._fields[key]
            previous = field.value

            if previous != value:
                self._changed.add(key)
                field.value = value

        else:
            object.__setattr__(self, key, value)

    def __getattr__(self, key):
        if key in self._fields:
            return self._fields[key].value

        return object.__getattribute__(self, key)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save()

    def save(self, force=False):
        exists = self._redis.exists(self._id)

        if exists and not force and not self._changed:
            return

        if not self._fetched and exists:
            raise Exception("Document already exists!")

        document_view = DocumentView(self)

        if callable(self.presentation):
            presentation = self.presentation(document_view)
        else:
            presentation = document_view

        self._redis.upsert(self._id, document_view)

        if any((
                force,
                self._presentational and self._changed & self._presentational,
                not self._presentational and self._changed
        )):
            self._firebase.update(self._path, self._id, presentation)

        if callable(self.on_save):
            self.on_save(document_view)

        self._changed = set()

    def delete(self):
        if self._redis.exists(self._id):
            self._redis.delete(self._id)
            self._firebase.delete(self._path, self._id)

            if callable(self.on_delete):
                document_view = DocumentView(self)
                self.on_delete(document_view)

    @classmethod
    def presentation(cls, document):
        return {
            key: value
            for key, value in document
            if key in cls._presentational
        }

    on_save = None
    on_presentation_save = None  # NOTE: Inactive yet

    on_delete = None


class DocumentView(dict):
    # TODO: Add __changed__ field
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
    # TODO: Create another metaclass
    # TODO: Make fields presentational by default for PresentationDocument

    def __init__(self, ignore_non_existing=False, override=False, **kwargs):
        super().__init__(ignore_non_existing, override, **kwargs)

        self._view_id = self._id
        self._id = "{}::{}".format(
            self.__class__.__name__,
            self._view_id
        )

    def save(self, force=False):
        if not force and not self._changed:
            return

        document_view = DocumentView(self)
        presentation = self.presentation(document_view)

        self._redis.upsert(self._id, self._view_id)
        self._firebase.update(self._path, self._view_id, presentation)

    def delete(self):
        if self._redis.exists(self._id):
            self._redis.delete(self._id)
            self._firebase.delete(self._path, self._view_id)

    @classmethod
    @abstractmethod
    def presentation(cls, document: DocumentView):
        pass
