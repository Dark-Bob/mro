import datetime
import uuid as _uuid
import json as json_
import psycopg2.extras
from mro.mro_dict import MroDict
from mro.mro_list import MroList
from mro.helpers import parse_to_mro_objects, mro_objects_to_json


# Create data type functions
def defaultColumnToDataType(column, code_start, code_end):
    return code_start + code_end


def varcharColumnToDataType(column, code_start, code_end):
    character_maximum_length = column[8]
    return f'{code_start}{character_maximum_length}, {code_end}'


# Create transform functions for converting fetched data
def default_convert(column, instance, data):
    return data


def json_convert(column, instance, data):
    return parse_to_mro_objects(column, instance, data)


# Create transform functions for default values
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


def json_transform(column_default, data_type):
    if column_default.endswith('::' + data_type):
        column_default = f'{column_default[1:-(len(data_type)+3)]}'
    column_default = json_.loads(column_default)
    return parse_to_mro_objects(None, None, column_default), False


type_map = {
    'character varying': ['varchar', varcharColumnToDataType, default_transform, default_convert],
    'integer': ['integer', defaultColumnToDataType, integer_transform, default_convert],
    'timestamp without time zone': ['timestamp', defaultColumnToDataType, none_transform, default_convert],
    'time without time zone': ['time', defaultColumnToDataType, none_transform, default_convert],
    'date': ['date', defaultColumnToDataType, none_transform, default_convert],
    'boolean': ['boolean', defaultColumnToDataType, boolean_transform, default_convert],
    'bool': ['boolean', defaultColumnToDataType, boolean_transform, default_convert],
    'text': ['text', defaultColumnToDataType, default_transform, default_convert],
    'double precision': ['double', defaultColumnToDataType, float_transform, default_convert],
    'real': ['real', defaultColumnToDataType, float_transform, default_convert],
    'json': ['json', defaultColumnToDataType, json_transform, json_convert],
    'jsonb': ['json', defaultColumnToDataType, json_transform, json_convert],
    'uuid': ['uuid', defaultColumnToDataType, default_transform, default_convert],
    'bytea': ['bytea', defaultColumnToDataType, default_transform, default_convert],
    'oid': ['oid', defaultColumnToDataType, integer_transform, default_convert],
    }


def convert_numpy_to_python(value):
    if hasattr(value, 'dtype'):
        value = value.item()
    return value


class database_type(object):

    def __init__(self, name, python_type, column_index, not_null, is_updateable, get_value_on_insert, is_primary_key, conversion_function):
        self.name = name
        self.python_type = python_type
        self.column_index = column_index
        self.not_null = not_null
        self.is_updateable = is_updateable
        self.get_value_on_insert = get_value_on_insert
        self.is_primary_key = is_primary_key
        self.conversion_function = conversion_function

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
            # may need to move out into derived class or create another layer for basic types
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

psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(list, psycopg2.extras.Json)


class json(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, str, column_index, **kwargs)

    def __get__(self, instance, instance_type):
        if instance is None:
            return self
        if self.name in instance.__dict__:
            return instance.__dict__[self.name]
        return None

    def __set__(self, instance, value):
        if not self.is_updateable:
            raise PermissionError(f'The value of [{self.name}] is not updateable.')
        if value is None:
            if self.not_null:
                raise ValueError(f'The value of [{self.name}] cannot be null.')
            result_obj = None
        elif type(value) in [MroList, MroDict]:
            # In this case they're already mro objects so we don't need to validate their json
            result_obj = value
        else:
            # may need to move out into derived class or create another layer for basic types
            if type(value) is not self.python_type:
                raise TypeError(f'Value should be of type [{self.python_type.__name__}] not [{value.__class__.__name__}]')
            self.validate_set(value)

            try:
                # Parse the values being set
                value = json_.loads(value)
                result_obj = parse_to_mro_objects(self, instance, value)
            except json_.decoder.JSONDecodeError as e:
                raise ValueError(f"Invalid input syntax for type json, provided input: {value}")
        # TODO: discuss this change with andy, it seems instance.update sets the instance.__dict__[self.name] so when
        # these lines are inverted the first line is essentially overwritten with the value as a string
        instance.update(**{self.name: mro_objects_to_json(result_obj)})
        instance.__dict__[self.name] = result_obj

    def validate_set(self, value):
        pass


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

class oid(database_type):

    def __init__(self, name, column_index, **kwargs):
        super().__init__(name, int, column_index, **kwargs)