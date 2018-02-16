import abc
import datetime


class FieldMeta(abc.ABCMeta):
    def __new__(mcls, name, bases, namespace, **kwargs):
        if '__field_type__' not in namespace:
            raise AttributeError("Field must have __field_type__ attribute!")

        return super().__new__(mcls, name, bases, namespace, **kwargs)


class BaseField(metaclass=FieldMeta):
    __field_type__ = None

    def __init__(self, id=False):
        self._value = None
        self._id = id

    @property
    def is_id(self):
        return self._id

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        if value and not isinstance(value, self.__field_type__):
            try:
                self._value = self.__field_type__(value)
                return
            except Exception as exception:
                raise TypeError("{}: Wrong field type <{}>. Should be <{}>.".format(
                    self.__class__.__name__,
                    value.__class__.__name__,
                    self.__field_type__.__name__
                )) from exception

        self._value = value


class AnyField(BaseField):
    __field_type__ = object


class StringField(BaseField):
    __field_type__ = str


class IntField(BaseField):
    __field_type__ = int


class FloatField(BaseField):
    __field_type__ = float


class DictField(BaseField):
    __field_type__ = dict


class BooleanField(BaseField):
    __field_type__ = bool


class DateField(BaseField):
    __field_type__ = datetime.datetime

