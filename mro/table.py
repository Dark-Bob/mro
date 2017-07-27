
import mro.connection as con
import mro.data_types
import mro.foreign_keys
import time
import psycopg2
import logging
import threading

logger = logging.getLogger(__name__)

# TODO replace with green thead friendly, thread local storage
psycopg2_lock = threading.Lock()

class table(object):

    _disable_insert =False

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
            except psycopg2.InterfaceError as e:
                if retry_count == 3:
                    raise
                logger.exception("Connection failure while getting cursor, will attempt to reconnect.")
                time.sleep(retry_count * 1)
                con.reconnect()
            except Exception as e:
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
                except psycopg2.InterfaceError as e:
                    if retry_count == 3:
                        raise
                    logger.exception("Connection failure will attempt to reconnect [{}] {}".format(sql, values))
                    time.sleep(retry_count * 1)
                    con.reconnect()
                    cursor = con.connection.cursor()
                except Exception as e:
                    logger.exception("Exception while executing sql [{}] {}".format(sql, values))
                    con.connection.rollback()
                    raise
                retry_count += 1

    @classmethod
    def select(cls, clause = None):
        cursor = cls._get_cursor()
        if clause is None:
            sql = "select * from \"{}\";".format(cls.__name__)
        else:
            sql = "select * from \"{}\" where {};".format(cls.__name__, clause)

        cls._execute_sql(sql, cursor=cursor)

        column_names = [column.name for column in cursor.description]

        table._disable_insert =True
        objs = []
        for row in cursor:
            kwargs = {}
            for index in range(len(column_names)):
                kwargs[column_names[index]] = row[index]
            objs.append(cls(**kwargs))
        table._disable_insert =False

        return objs

    @classmethod
    def select_count(cls, clause = None):
        cursor = cls._get_cursor()
        if clause is None:
            sql = "select count(*) from \"{}\";".format(cls.__name__)
        else:
            sql = "select count(*) from \"{}\" where {};".format(cls.__name__, clause)

        cls._execute_sql(sql, cursor=cursor)

        for row in cursor:
            return row[0]

    @classmethod
    def select_one(cls, clause = None):
        cursor = cls._get_cursor()
        if clause is None:
            sql = "select * from \"{}\" limit 1;".format(cls.__name__)
        else:
            sql = "select * from \"{}\" where {} limit 1;".format(cls.__name__, clause)

        cls._execute_sql(sql, cursor=cursor)

        column_names = [column.name for column in cursor.description]

        obj = None
        table._disable_insert =True
        for row in cursor:
            kwargs = {}
            for index in range(len(column_names)):
                kwargs[column_names[index]] = row[index]
            obj = cls(**kwargs)
        table._disable_insert =False

        return obj

    @classmethod
    def delete(cls, clause = None):
        if clause is None:
            sql = "delete from \"{}\";".format(cls.__name__)
        else:
            sql = "delete from \"{}\" where {};".format(cls.__name__, clause)

        cls._execute_sql(sql)

    @classmethod
    def insert(cls, **kwargs):
        if table._disable_insert:
            return

        cursor = cls._get_cursor()
        
        keys = kwargs.keys()
        if len(keys) == 0:
            cols = 'default'
            vals_str = ''
            vals = ()
        else:
            cols = '({})'.format(', '.join(list(keys)))
            vals = list(kwargs.values())
            vals_str_list = ["%s"] * len(vals)
            vals_str = ' ({})'.format(', '.join(vals_str_list))

        if cls._get_value_on_insert_columns_str:
            sql = "insert into \"{t}\" {c} values{v} returning {c2}".format(
                t = cls.__name__, c = cols, v = vals_str, c2 = cls._get_value_on_insert_columns_str)

            cls._execute_sql(sql, vals, cursor=cursor)

            table._disable_insert = True
            for row in cursor:
                for index in range(len(cls._get_value_on_insert_columns)):
                    kwargs[cls._get_value_on_insert_columns[index]] = row[index]
                obj = cls(**kwargs)
            table._disable_insert = False
        else:
            sql = "insert into \"{t}\" {c} values{v}".format(
                t = cls.__name__, c = cols, v = vals_str)

            cls._execute_sql(sql, vals, cursor=cursor)

            table._disable_insert = True
            obj = cls(**kwargs)
            table._disable_insert = False

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
        if table._disable_insert:
            return

        if not match_columns:
            raise ValueError("Update needs columns to match to update, is your table missing a prmary key?")

        vals = list(kwargs.values()) + match_column_values
        update_column_str = ", ".join([c + '=%s' for c in kwargs.keys()])
        match_column_str = " and ".join([c + '=%s' for c in match_columns])
        sql = "update \"{t}\" set {c} where {c2}".format(
            t = cls.__name__, c = update_column_str, c2 = match_column_str)

        cls._execute_sql(sql, vals)


    @classmethod
    def update_many(cls, match_columns, match_column_values, update_columns, update_column_values):
        #update test as t set
        #    column_a = c.column_a
        #from (values
        #    ('123', 1),
        #    ('345', 2)  
        #) as c(column_b, column_a) 
        #where c.column_b = t.column_b;
        raise Exception('Not implemented')