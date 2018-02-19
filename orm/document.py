import abc
import copy

from .fields import BaseField

from orm import connections, adapters


class DocumentMetaclass(abc.ABCMeta):
    def __new__(mcs, name, bases, attrs):
        conn = attrs.get('_connection')
        if isinstance(conn, dict) and {'redis', 'firebase'} <= conn.keys():
            redis_props = conn['redis']
            firebase_props = conn.get('firebase', {})
            attrs['_container'] = firebase_props.pop('container', '')

            redis_adapter, firebase_adapter = adapters()

            attrs.update({
                '_redis': redis_adapter(**redis_props) if redis_props else None,
                '_firebase': firebase_adapter(**firebase_props) if firebase_props else None
            })

        fields = {}
        fieldnames = [
            name for name, item in attrs.items()
            if isinstance(item, BaseField)
        ]

        presentation_fields = set()

        for fieldname in fieldnames:
            field = attrs[fieldname]
            fields[fieldname] = field

            if field.presentation:
                presentation_fields.add(fieldname)

            if field.is_id:
                fields['id'] = field

            del attrs[fieldname]

        attrs['__fields__'] = fields
        attrs['_required_fields'] = set(
            name for name, field in fields.items()
            if field.required
        )

        # If we class with "presentation" in bases, then all of its fields are presentational
        # Should think of a better way

        if any("presentation" in repr(base).lower() for base in bases):
            attrs['_presentation_fields'] = set(fieldnames)
        else:
            attrs['_presentation_fields'] = presentation_fields

        return super().__new__(mcs, name, bases, attrs)


class Document(metaclass=DocumentMetaclass):
    def __new__(cls, *args, **kwargs):
        document = super().__new__(cls)

        if not ({'_redis', '_firebase'} <= cls.__dict__.keys()):
            redis, firebase = connections()
            if not redis and not firebase:
                raise ConnectionError('Connections to Firebase and Redis not found')

            # TODO: Make them class-level instead of instance-level (Maybe move to metaclass)
            object.__setattr__(document, '_redis', redis)
            object.__setattr__(document, '_firebase', firebase)

        return document

    def __init__(self, ignore_non_existing=False, override=False, **kwargs):
        cls = self.__class__
        fields = copy.deepcopy(cls.__fields__)
        required_fields = cls._required_fields

        document_id, filled_fields = self._init_fields(fields, ignore_non_existing=ignore_non_existing, **kwargs)

        if document_id is None:
            raise LookupError("Document without id!")

        if (filled_fields & required_fields) != required_fields:
            raise AttributeError("The following fields are required: {}".format(
                required_fields - filled_fields
            ))

        self._init_options(ignore_non_existing=ignore_non_existing, override=override)

    @classmethod
    def get(cls, id):
        if id is None:
            raise AttributeError("Document must have id field!")

        document = cls.__new__(cls)
        document._init_fields(cls.__fields__)
        document._init_options(
            ignore_non_existing=False,
            override=False
        )

        if document._redis.exists(id):
            object.__setattr__(document, '_id', id)
            return document._fetch()

        raise LookupError('No such document with id: {}'.format(id))

    def _init_fields(self, fields, ignore_non_existing=False, **kwargs):
        filled_fields = set()
        for name, value in kwargs.items():
            field = fields.get(name)

            if field is None:
                if ignore_non_existing:
                    continue

                raise AttributeError("No such field {}".format(name))

            field.value = value
            filled_fields.add(name)

        document_id = fields['id'].value

        object.__setattr__(self, '_id', document_id)
        object.__setattr__(self, '_fields', fields)

        return document_id, filled_fields

    def _init_options(self, ignore_non_existing, override):
        self._ignore_non_existing = ignore_non_existing
        self._path = "{}/".format(self._container)
        self._fetched = override
        self._changed = set(self._fields.keys())

    def _fetch(self):
        entity = self._redis.read(self._id)
        for key, value in entity.items():
            if key in self._fields:
                self._fields[key].value = value
            elif not self._ignore_non_existing:
                raise AttributeError("There is no such field as {}".format(key))

        self._fetched = True
        self._changed = set()

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
            raise Exception("Document with id {} already exists!".format(self._id))

        document_view = DocumentView(self)

        if callable(self.presentation):
            presentation = self.presentation(document_view)
        else:
            presentation = document_view

        self._redis.upsert(self._id, document_view)

        if any((
                force,
                self._presentation_fields and self._changed & self._presentation_fields,
                not self._presentation_fields and self._changed,
                not exists
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
            for key, value in document.items()
            if key in cls._presentation_fields
        }

    on_save = None
    on_delete = None


class DocumentView(dict):
    def __init__(self, document: Document):
        super().__init__()

        self.update({
            key: item.value
            for key, item in document._fields.items()
        })

        self.__changed__ = frozenset(document._changed)
        self.__presentation_changed__ = frozenset(document._changed & document._presentation_fields)

    def __getattr__(self, item):
        if item in self:
            return self[item]

        raise AttributeError("No such attribute: {}!".format(item))


class PresentationDocument(Document):
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
