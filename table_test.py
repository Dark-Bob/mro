import os
import pytest
import mro
import connection as con
from datetime import datetime, date, time

class table1(mro.table.table):

    column1 = mro.data_types.integer('column1', 0, not_null=False, is_updatable=True, get_value_on_insert = False, is_primary_key = False)
    column2 = mro.data_types.varchar('column2', 1, 20, not_null=False, is_updatable=True, get_value_on_insert = False, is_primary_key = False)
    column3 = mro.data_types.integer('column3', 1, not_null=False, is_updatable=True, get_value_on_insert = False, is_primary_key = False)

    def __init__(self, **kwargs):
        self.__dict__['column1'] = 1
        self.__dict__['column2'] = None
        self.__dict__['column3'] = None
        for k, v in kwargs.items():
            if not hasattr(self, k):
                raise ValueError("{} dos not have an attribute {}".format(self.__class__.__name__, k))
            self.__dict__[k] = v

        if not table1._disable_insert:
            obj = super().insert(**kwargs)
            for c in table1._get_value_on_insert_columns:
                self.__dict__[c] = obj.__dict__[c]

    def update(self, **kwargs):
        primary_key_columns = table1._primary_key_columns
        primary_key_column_values = [self.__dict__[c] for c in primary_key_columns]

        super().update(primary_key_columns, primary_key_column_values, **kwargs)

table1._register()

@pytest.fixture
def connection_function(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("create table table1 (id serial primary key, created_date date not null default current_date, column1 integer default 1, column2 varchar(20), column3 integer)")
    cursor.execute("create table table2 (column1 varchar(20), column2 integer, column3 varchar(20))")
    cursor.execute("create table table3 (created_datetime timestamp not null default current_timestamp, created_time time not null default current_time, column1 varchar(20) default 'ABC DEF', column2 integer, column3 varchar(20), column4 jsonb)")
    cursor.execute("insert into table1 (column1, column2, column3) values (%s,%s,%s)", (1,'Hello World!', 2))
    cursor.execute("insert into table1 (column1, column2, column3) values (%s,%s,%s)", (2,'Hello World2!', 3))
    cursor.execute("insert into table2 values (%s,%s,%s)", ('Hello World3!', 4, 'Hello World4!'))
    connection.commit()
    connection.close()

    return lambda: con.connect()

class TestTable(object):

    def test_table_reflection(self, connection_function):
        mro.load_database(connection_function)

        tables = mro.table1.select()

        assert len(tables) == 2

        assert isinstance(tables[0].created_date, date)
        assert tables[0].column1 == 1
        assert tables[0].column2 == 'Hello World!'
        assert tables[0].column3 == 2

        assert tables[1].column1 == 2
        assert tables[1].column2 == 'Hello World2!'
        assert tables[1].column3 == 3

        tables = mro.table2.select()

        assert len(tables) == 1

        assert tables[0].column1 == 'Hello World3!'
        assert tables[0].column2 == 4
        assert tables[0].column3 == 'Hello World4!'

    def test_table_select_filter(self, connection_function):
        mro.load_database(connection_function)

        tables = mro.table1.select('column1 = %d' % 2)

        assert len(tables) == 1

        assert tables[0].column1 == 2
        assert tables[0].column2 == 'Hello World2!'
        assert tables[0].column3 == 3

        tables = mro.table2.select("column1 = '%d'" % 1)

        assert len(tables) == 0

    def test_table_delete_filter(self, connection_function):
        mro.load_database(connection_function)

        table_count = mro.table1.select_count()
        tables = mro.table1.select('column1 = %d' % 2)

        assert len(tables) == 1

        assert tables[0].column1 == 2
        assert tables[0].column2 == 'Hello World2!'
        assert tables[0].column3 == 3

        mro.table1.delete('column1 = %d' % 2)

        assert table_count - 1 == mro.table1.select_count()

    def test_create_object(self, connection_function):
        mro.load_database(connection_function)

        table_count = mro.table1.select_count()

        table = mro.table1(column1 = 3, column2 = 'Hi!', column3 = 11)

        assert table.column1 == 3
        assert table.column2 == 'Hi!'
        assert table.column3 == 11

        table = mro.table1(column2 = 'Hi2!')

        assert table.column1 == 1
        assert table.column2 == 'Hi2!'
        assert table.column3 == None

        kwargs = {'column1': 5, 'column2': 'Hi3!', 'column3': 78}
        table = mro.table1(**kwargs)

        assert table.column1 == 5
        assert table.column2 == 'Hi3!'
        assert table.column3 == 78

        tables = mro.table1.select()

        assert table_count + 3 ==len(tables)        

        assert tables[4].column1 == 5
        assert tables[4].column2 == 'Hi3!'
        assert tables[4].column3 == 78

    def test_insert_check_default_values(self, connection_function):

        mro.load_database(connection_function)

        table_count = mro.table1.select_count()

        table = mro.table1(column1 = 3, column2 = 'Hi!')

        table = mro.table1(column1 = 3, column2 = 'Hi!', column3 = 11, created_date = datetime.now().date())

        tables = mro.table1.select()

        for table in tables:
            assert isinstance(table.id, int)
            assert table.id != None
            assert isinstance(table.created_date, date)
            assert table.created_date != None
            assert isinstance(table.column1, int)
            assert table.column1 != None
            assert isinstance(table.column2, str)
            assert table.column2 != None
            assert table.column3 == None or isinstance(table.column3, int)

        table = mro.table3(column3 = 'Hi56!', column4 = '{"data": 1}')

        table = mro.table3.select_one("column3 = 'Hi56!'")

        assert isinstance(table.column1, str)
        assert table.column1 == 'ABC DEF'
        assert isinstance(table.column3, str)
        assert table.column3 != None
        assert isinstance(table.column4, str)
        assert table.column4 != None

    def test_insert_many(self, connection_function):
        mro.load_database(connection_function)

        mro.table1.delete()

        table = mro.table1.insert_many(
            ['column1', 'column2', 'column3'],
            [
                [1, 'Hi!', 7],
                [2, 'Hi2!', 13],
                [3, 'Hi3!', 21]
            ])

        tables = mro.table1.select()

        assert 3 == len(tables)   

        assert tables[0].column1 == 1
        assert tables[0].column2 == 'Hi!'
        assert tables[0].column3 == 7

        assert tables[1].column1 == 2
        assert tables[1].column2 == 'Hi2!'
        assert tables[1].column3 == 13

        assert tables[2].column1 == 3
        assert tables[2].column2 == 'Hi3!'
        assert tables[2].column3 == 21

    def test_insert_with_only_primary_key_no_kwargs(self, connection_function):
        mro.load_database(connection_function)

        table_count = mro.table1()

if __name__ == '__main__':
    #pytest.main([__file__])
    pytest.main([__file__ + '::TestTable::test_insert_with_only_primary_key_no_kwargs'])