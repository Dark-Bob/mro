
import psycopg2.extras
import datetime
import uuid as _uuid


# Create data type functions
def defaultColumnToDataType(column, code_start, code_end):
    return code_start + code_end


def varcharColumnToDataType(column, code_start, code_end):
    character_maximum_length = column[8]
    return f'{code_start}{character_maximum_length}, {code_end}'


# Create transform functions
def default_transform(column_default, data_type):
    if column_default.endswith('::' + data_type):
        column_default = f'{column_default[1:-(len(data_type)+3)]}'
    return column_default, False


def float_transform(column_default, data_type):
    return float(column_default), False


def integer_transform(column_default, data_type):
    if column_default.endswith('::regclass)'):
        return None, True
    return int(column_default), False


def boolean_transform(column_default, data_type):
    def str2bool(value: str):
        return {'true': True,
                'false': False
                }.get(value.lower(), None)
    result = str2bool(column_default)
    return result, False


def none_transform(column_default, data_type):
    return None, True


type_map = {
    'character varying': ['varchar', varcharColumnToDataType, default_transform],
    'integer': ['integer', defaultColumnToDataType, integer_transform],
    'timestamp without time zone': ['timestamp', defaultColumnToDataType, none_transform],
    'time without time zone': ['time', defaultColumnToDataType, none_transform],
    'date': ['date', defaultColumnToDataType, none_transform],
    'boolean': ['boolean', defaultColumnToDataType, boolean_transform],
    'bool': ['boolean', defaultColumnToDataType, boolean_transform],
    'text': ['text', defaultColumnToDataType, default_transform],
    'double precision': ['double', defaultColumnToDataType, float_transform],
    'real': ['real', defaultColumnToDataType, float_transform],
    'json': ['json', defaultColumnToDataType, default_transform],
    'jsonb': ['json', defaultColumnToDataType, default_transform],
    'uuid': ['uuid', defaultColumnToDataType, default_transform],
    'bytea': ['bytea', defaultColumnToDataType, default_transform],
    }


def convert_numpy_to_python(value):
    if hasattr(value, 'dtype'):
        value = value.item()
    return value


class database_type(object):

    def __init__(self, name, python_type, column_index, not_null, is_updateable, get_value_on_insert, is_primary_key):
        self.name = name
        self.python_type = python_type
        self.column_index = column_index
        self.not_null = not_null
        self.is_updateable = is_updateable
        self.get_value_on_insert = get_value_on_insert
        self.is_primary_key = is_primary_key

    def __get__(self, instance, instance_type):
        if instance is None:
            return self
        if self.name in instance.__dict__:
            return instance.__dict__[self.name]
        return None

    def __set__(self, instance, value):
        value = convert_numpy_to_python(value)
        if not self.is_updateable:
            raise PermissionError(f'The value of [{self.name}] is not updateable.')
        if value is None:
            if self.not_null:
                raise ValueError(f'The value of [{self.name}] cannot be null.')
        else:
            # may need to move out into derived class or ceate another layer for basic types
            if type(value) is not self.python_type:
                raise TypeError(f'Value should be of type [{self.python_type.__name__}] not [{value.__class__.__name__}]')
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
            raise ValueError(f'Value length [{len(value)}] should not exceed [{self.length}]')


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