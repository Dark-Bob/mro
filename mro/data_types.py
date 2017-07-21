
import psycopg2.extras
import datetime
import uuid as _uuid

class database_type(object):

    def __init__(self, name, python_type, column_index, not_null, is_updatable, get_value_on_insert, is_primary_key):
        self.name = name
        self.python_type = python_type
        self.column_index = column_index
        self.not_null = not_null
        self.is_updatable = is_updatable
        self.get_value_on_insert = get_value_on_insert
        self.is_primary_key = is_primary_key

    def __get__(self, instance, instance_type):
        if instance is None:
            return self
        if self.name in instance.__dict__:
            return instance.__dict__[self.name]
        return None

    def __set__(self, instance, value):
        if not self.is_updatable:
            raise PermissionError('The value of [{}] is not updateable.'.format(self.name))
        if value is None:
            if self.not_null:
                raise ValueError('The value of [{}] cannot be null.'.format(self.name))
        else:
            # may need to move out into derived class or ceate another layer for basic types
            if type(value) is not self.python_type:
                raise TypeError('Value should be of type [{}] not [{}]'.format(self.python_type.__name__, value.__class__.__name__))
            self.validate_set(value)
        instance.__dict__[self.name] = value
        instance.update(**{self.name: value})

    def validate_set(self, value):
        pass

class varchar(database_type):

    def __init__(self, name, column_index, length, **kwargs):
        super().__init__(name, str, column_index, **kwargs)
        self.length = length

    def validate_set(self, value):
        if len(value) > self.length:
            raise ValueError('Value length [{}] should not exceed [{}]'.format(len(value), self.length))

class integer(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, int, column_index, **kwargs)

class timestamp(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, datetime.datetime, column_index, **kwargs)

class date(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, datetime.date, column_index, **kwargs)

class time(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, datetime.time, column_index, **kwargs)

class boolean(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, bool, column_index, **kwargs)

class text(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, str, column_index, **kwargs)

# stop automatic conversion of json into a dictionary type
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extras.register_default_jsonb(loads=lambda x: x)
class json(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, str, column_index, **kwargs)

class double(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, float, column_index, **kwargs)

class real(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, float, column_index, **kwargs)

class uuid(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, _uuid.UUID, column_index, **kwargs)

class bytea(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, bytes, column_index, **kwargs)

class blob(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, bytes, column_index, **kwargs)