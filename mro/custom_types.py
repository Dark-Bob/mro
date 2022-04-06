import mro
import datetime
import csv
import psycopg2.extensions, psycopg2.extras
from operator import attrgetter


def _get_custom_type_names(connection):
    cursor = connection.cursor()
    # Get custom types
    # 'lo' is a type that comes from the lo extension
    cursor.execute("""SELECT t.typname as type
                                FROM pg_type t
                                LEFT JOIN pg_catalog.pg_namespace n
                                       ON n.oid = t.typnamespace
                                WHERE 
                                (
                                      t.typrelid = 0 
                                      OR 
                                      (
                                           SELECT c.relkind = 'c' 
                                           FROM pg_catalog.pg_class c
                                           WHERE c.oid = t.typrelid
                                      )
                                ) 
                                AND NOT EXISTS
                                (
                                      SELECT 1
                                      FROM pg_catalog.pg_type el 
                                      WHERE el.oid = t.typelem
                                      AND el.typarray = t.oid
                                )
                                AND n.nspname NOT IN 
                                (
                                      'pg_catalog',
                                      'information_schema'
                                )
                                AND t.typname != 'lo'
                        """)
    connection.commit()
    types = [t for t in cursor]
    return types


def customColumnToDataType(column, code_start, code_end):
    python_type = f'mro.custom_types.{column[27]}'
    return '{0}{1}, {2}'.format(code_start, python_type, code_end)


def strtobool(value: str):
    return {'true': True,
            'false': False,
            't': True,
            'f': False
            }.get(value.lower(), None)


def strtodate(value: str):
    return datetime.datetime.strptime(value, '%Y-%m-%d').date()


def strtotime(value: str):
    return datetime.datetime.strptime(value, '%H:%M:%S.%f').time()


def strtotimestamp(value: str):
    return datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')


postgres_type_to_python_map = {
    'character varying': str,
    'text': str,
    'integer': int,
    'boolean': strtobool,
    'bool': strtobool,
    'float': float,
    'double precision': float,
    'real': float,
    'date': strtodate,
    'timestamp without time zone': strtotimestamp,
    'time without time zone': strtotime,
    'json': str,
    'jsonb': str,
    'uuid': str,
    'bytea': str,
    'oid': int
}


class CustomFieldType:
    def __init__(self, attribute):
        self.attribute = attribute

    def __get__(self, instance, type):
        if instance is None:
            return self
        else:
            return attrgetter(self.attribute)(instance)

    def __set__(self, instance, value):
        raise NotImplementedError("You cannot set custom type internal attributes")


def create_custom_types(connection):
    custom_types = _get_custom_type_names(connection)

    for custom_type in custom_types:
        type_name = custom_type[0]
        if type_name == 'hstore' or type_name == 'ghstore':
            continue
        mro.data_types.type_map[type_name] = [type_name, customColumnToDataType, mro.data_types.default_transform, mro.data_types.default_convert]
        psycopg2.extras.register_composite(type_name, connection)

        cursor2 = connection.cursor()
        cursor2.execute(f"""
                          SELECT a.attname AS "Field",
                                 pg_catalog.format_type(a.atttypid, a.atttypmod) AS "Datatype"
                          FROM pg_attribute AS a
                          WHERE attrelid IN
                          (
                             SELECT typrelid
                             FROM pg_type
                             WHERE typname = '{type_name}'
                          );
                       """)
        connection.commit()
        fields = [field for field in cursor2]

        # Create the python custom class
        def create_custom_type(name, fields):
            def constructor(self, **kwargs):
                for k, v in kwargs.items():
                    self.__dict__[f"_{k}"] = v

            custom_type_dict = {'__init__': constructor}
            for field in fields:
                column_name = field[0]
                custom_type_dict[column_name] = None

            new_custom_class = type(name, (), custom_type_dict)
            for field in fields:
                setattr(new_custom_class, field[0], CustomFieldType(f"_{field[0]}"))

            def cast_custom(value, cur):
                if value is None:
                    return None
                value = value.strip('(').rstrip(')')
                values = list(csv.reader([value]))[0]
                result_dict = {}
                for i in range(len(fields)):
                    col_name = fields[i][0]
                    col_type = fields[i][1]
                    col_value = values[i]
                    if len(values[i]) == 0:
                        result_dict[col_name] = None
                    else:
                        result_dict[col_name] = postgres_type_to_python_map[col_type](col_value)
                return new_custom_class(**result_dict)

            cursor3 = connection.cursor()
            # Get the postgres type object id for this custom type
            cursor3.execute(f"""
                                SELECT pg_type.oid
                                  FROM pg_type
                                      JOIN pg_namespace
                                        ON typnamespace = pg_namespace.oid
                                 WHERE typname = '{type_name}'
                                   AND nspname = 'public'
                                   """)
            connection.commit()
            custom_object_oid = cursor3.fetchone()[0]
            new_custom_type = psycopg2.extensions.new_type((custom_object_oid,), new_custom_class.__name__,
                                                           cast_custom)
            psycopg2.extensions.register_type(new_custom_type, connection)

            def adapt_custom_type(custom_type):
                fields = []
                for k, v in custom_type.__dict__.items():
                    fields.append(str(v))
                return psycopg2.extensions.AsIs(str(tuple(fields)))

            psycopg2.extensions.register_adapter(new_custom_class, adapt_custom_type)
            return new_custom_class

        custom_python_class = create_custom_type(type_name, fields)
        setattr(mro.custom_types, custom_python_class.__name__, custom_python_class)

        # Create the database custom class
        def custom_type_constructor(self, name, column_index, python_type, **kwargs):
            super(self.__class__, self).__init__(name, python_type, column_index, **kwargs)

        def setter(self, instance, value):
            if not self.is_updateable:
                raise PermissionError('The value of [{}] is not updateable.'.format(self.name))
            if value is None:
                if self.not_null:
                    raise ValueError('The value of [{}] cannot be null.'.format(self.name))
            elif type(value) is not self.python_type:
                if type(value) is dict or type(value) is tuple:
                    value = self.python_type(**value)
                else:
                    raise TypeError(
                        'Value should be of type [{}] or dictionary not [{}]'.format(self.python_type.__name__,
                                                                                     value.__class__.__name__))
                self.validate_set(value)
            instance.__dict__[self.name] = value
            instance.update(**{self.name: value})

        attrib_dict = {'__init__': custom_type_constructor,
                       '__set__': setter}
        for field in fields:
            attrib_dict[field] = None
        custom_db_class = type(type_name,
                               (mro.data_types.database_type,),
                               attrib_dict)
        setattr(mro.data_types, custom_db_class.__name__, custom_db_class)

