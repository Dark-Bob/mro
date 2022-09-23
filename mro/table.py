from contextlib import contextmanager
import logging
import threading
import time

import psycopg2
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_random_exponential

import mro.connection as con
import mro.data_types
import mro.foreign_keys
from mro.mro_dict import MroDict
from mro.mro_list import MroList
from mro.helpers import mro_objects_to_json

logger = logging.getLogger(__name__)

# TODO replace with green thread friendly, thread local storage
psycopg2_lock = threading.Lock()


MAX_ATTEMPTS = 3


@contextmanager
def disable_insert():
    table._insert.disabled = True
    try:
        yield
    finally:
        table._insert.disabled = False


class insert_local(threading.local):
    disabled = False


class table(object):
    _insert = insert_local()

    @classmethod
    def _register(cls):
        data_types = []
        foreign_keys = []
        for key, value in cls.__dict__.items():
            if isinstance(value, mro.data_types.database_type):
                data_types.append(value)
            if isinstance(value, mro.foreign_keys.foreign_key_data_type):
                foreign_keys.append(value)
        cls._get_value_on_insert_columns = [d.name for d in data_types if d.get_value_on_insert]
        cls._get_value_on_insert_columns_str = ', '.join(cls._get_value_on_insert_columns)
        cls._primary_key_columns = [d.name for d in data_types if d.is_primary_key]

    @classmethod
    def _get_cursor(cls):
        retry_count = 0
        while True:
            try:
                return con.connection.cursor()
            except psycopg2.InterfaceError:
                if retry_count == MAX_ATTEMPTS:
                    raise
                logger.exception("Connection failure while getting cursor, will attempt to reconnect.")
                time.sleep(retry_count * 1)
                con.reconnect()
            except Exception:
                logger.exception("Exception while getting sql cursor.")
                raise
            retry_count += 1

    @classmethod
    def _execute_sql(cls, sql, values=None, cursor=None):
        with psycopg2_lock:
            if cursor is None:
                cursor = cls._get_cursor()
            retry_count = 0
            retry = True
            while retry:
                try:
                    cursor.execute(sql, values)
                    con.connection.commit()
                    retry = False
                except psycopg2.InterfaceError:
                    if retry_count == MAX_ATTEMPTS:
                        raise
                    logger.exception("Connection failure will attempt to reconnect [{}] {}".format(sql, values))
                    time.sleep(retry_count * 1)
                    con.reconnect()
                    cursor = con.connection.cursor()
                except Exception:
                    logger.exception("Exception while executing sql [{}] {}".format(sql, values))
                    try:
                        con.connection.rollback()
                    except psycopg2.InterfaceError:
                        logger.exception("Connection failure on attempt to rollback [{}] {}".format(sql, values))
                    raise
                retry_count += 1
            return cursor

    @staticmethod
    def _convert_numpy_types_to_python(values):
        for k, v in values.items():
            values[k] = mro.data_types.convert_numpy_to_python(v)
        return values

    @classmethod
    @retry(wait=wait_random_exponential(), stop=stop_after_attempt(MAX_ATTEMPTS),
           reraise=True, before_sleep=before_sleep_log(logger, logging.WARNING))
    def select(cls, clause=None, *format_args):
        format_args = [x if not isinstance(x, mro.foreign_keys.foreign_key) else x.value for x in format_args]

        if clause is None:
            sql = "select * from \"{}\";".format(cls.__name__)
        else:
            sql = "select * from \"{}\" where {};".format(cls.__name__, clause)
        cursor = cls._execute_sql(sql, values=format_args)

        column_names = [column.name for column in cursor.description]

        with disable_insert():
            objs = []
            for row in cursor:
                kwargs = {}
                for index in range(len(column_names)):
                    kwargs[column_names[index]] = row[index]
                objs.append(cls(**kwargs))
        return objs

    @classmethod
    @retry(wait=wait_random_exponential(), stop=stop_after_attempt(MAX_ATTEMPTS),
           reraise=True, before_sleep=before_sleep_log(logger, logging.WARNING))
    def select_count(cls, clause=None, *format_args):
        format_args = [x if not isinstance(x, mro.foreign_keys.foreign_key) else x.value for x in format_args]

        if clause is None:
            sql = "select count(*) from \"{}\";".format(cls.__name__)
        else:
            sql = "select count(*) from \"{}\" where {};".format(cls.__name__, clause)

        cursor = cls._execute_sql(sql, values=format_args)

        for row in cursor:
            return row[0]

    @classmethod
    @retry(wait=wait_random_exponential(), stop=stop_after_attempt(MAX_ATTEMPTS),
           reraise=True, before_sleep=before_sleep_log(logger, logging.WARNING))
    def select_one(cls, clause=None, *format_args):
        format_args = [x if not isinstance(x, mro.foreign_keys.foreign_key) else x.value for x in format_args]
        if clause is None:
            sql = "select * from \"{}\" limit 1;".format(cls.__name__)
        else:
            sql = "select * from \"{}\" where {} limit 1;".format(cls.__name__, clause)

        cursor = cls._execute_sql(sql, values=format_args)

        column_names = [column.name for column in cursor.description]

        obj = None

        with disable_insert():
            for row in cursor:
                kwargs = {}
                for index in range(len(column_names)):
                    kwargs[column_names[index]] = row[index]
                obj = cls(**kwargs)

        return obj

    @classmethod
    def delete(cls, clause=None, *format_args):
        format_args = [x if not isinstance(x, mro.foreign_keys.foreign_key) else x.value for x in format_args]
        if clause is None:
            sql = "delete from \"{}\";".format(cls.__name__)
        else:
            sql = "delete from \"{}\" where {};".format(cls.__name__, clause)

        cls._execute_sql(sql, values=format_args)

    @classmethod
    def insert(cls, **kwargs):
        if table._insert.disabled:
            return

        keys = list(kwargs.keys())
        if len(keys) == 0:
            cols = 'default'
            vals_str = ''
            vals = ()
        else:
            kwargs = table._convert_numpy_types_to_python(kwargs)
            cols = '({})'.format(', '.join(keys))
            vals = [x if not isinstance(x, mro.foreign_keys.foreign_key) else x.value for x in kwargs.values()]
            for i in range(len(vals)):
                if isinstance(cls.__dict__[keys[i]], mro.data_types.json) and not isinstance(vals[i], str):
                    vals[i] = mro_objects_to_json(vals[i])
            vals_str_list = ["%s"] * len(vals)
            vals_str = ' ({})'.format(', '.join(vals_str_list))

        if cls._get_value_on_insert_columns_str:
            sql = "insert into \"{t}\" {c} values{v} returning {c2}".format(
                t=cls.__name__, c=cols, v=vals_str, c2=cls._get_value_on_insert_columns_str)

            cursor = cls._execute_sql(sql, vals)

            with disable_insert():
                for row in cursor:
                    for index in range(len(cls._get_value_on_insert_columns)):
                        kwargs[cls._get_value_on_insert_columns[index]] = row[index]
                    obj = cls(**kwargs)
        else:
            sql = "insert into \"{t}\" {c} values{v}".format(
                t=cls.__name__, c=cols, v=vals_str)

            cls._execute_sql(sql, vals)

            with disable_insert():
                obj = cls(**kwargs)

        return obj

    @classmethod
    def insert_many(cls, cols, values):

        cursor = cls._get_cursor()

        cols = ', '.join(cols)
        vals_str_list = ["%s"] * len(values[0])
        vals_str = "({})".format(", ".join(vals_str_list))

        # speed up of
        # aggregate_values = ','.join(cursor.mogrify(vals_str, x).decode("utf-8")  for x in values)
        aggregate_values = cursor.mogrify(
            ','.join([vals_str for i in range(len(values))]),
            [item for sublist in values for item in sublist]).decode("utf-8")

        sql = "insert into \"{}\" ({}) values {}".format(
            cls.__name__, cols, aggregate_values)

        cls._execute_sql(sql, cursor=cursor)

    @classmethod
    def update(cls, match_columns, match_column_values, **kwargs):
        if table._insert.disabled:
            return

        if not match_columns:
            raise ValueError("Update needs columns to match to update, is your table missing a primary key?")

        keys = list(kwargs.keys())
        vals = [x if not isinstance(x, mro.foreign_keys.foreign_key) else x.value for x in kwargs.values()]
        for i in range(len(vals)):
            if isinstance(cls.__dict__[list(keys)[i]], mro.data_types.json) and not isinstance(vals[i], str):
                vals[i] = mro_objects_to_json(vals[i])

        vals = vals + match_column_values
        update_column_str = ", ".join([c + '=%s' for c in keys])
        match_column_str = " and ".join([c + '=%s' for c in match_columns])
        sql = "update \"{t}\" set {c} where {c2}".format(
            t=cls.__name__, c=update_column_str, c2=match_column_str)

        cls._execute_sql(sql, vals)

    @classmethod
    def update_many(cls, match_columns, match_column_values, update_columns, update_column_values):
        # update test as t set
        #    column_a = c.column_a
        # from (values
        #    ('123', 1),
        #    ('345', 2)
        # ) as c(column_b, column_a)
        # where c.column_b = t.column_b;
        raise Exception('Not implemented')