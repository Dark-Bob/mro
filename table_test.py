import pytest
import mro
import connection as con
from datetime import datetime, date
from threading import Thread, Event
from mro.mro_dict import MroDict

class table1(mro.table.table):

    column1 = mro.data_types.integer('column1', 0, not_null=False, is_updateable=True, get_value_on_insert = False, is_primary_key=False, conversion_function=lambda x: x)
    column2 = mro.data_types.varchar('column2', 1, 20, not_null=False, is_updateable=True, get_value_on_insert = False, is_primary_key=False, conversion_function=lambda x: x)
    column3 = mro.data_types.integer('column3', 1, not_null=False, is_updateable=True, get_value_on_insert = False, is_primary_key=False, conversion_function=lambda x: x)

    def __init__(self, **kwargs):
        self.__dict__['column1'] = 1
        self.__dict__['column2'] = None
        self.__dict__['column3'] = None
        for k, v in kwargs.items():
            if not hasattr(self, k):
                raise ValueError("{} does not have an attribute {}".format(self.__class__.__name__, k))
            self.__dict__[k] = v

        if not mro.table.disable_insert():
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

    cursor.execute("create table table1 (id serial primary key, created_date date not null default current_date, column1 integer default 1, column2 varchar(20), column3 integer, column4 float default 1.2, column5 bool default False, column6 oid default 999)")
    cursor.execute("create table table2 (column1 varchar(20), column2 integer, column3 varchar(20))")
    cursor.execute("create table table3 (created_datetime timestamp not null default current_timestamp, created_time time not null default current_time, column1 varchar(20) default 'ABC DEF', column2 integer, column3 varchar(20), column4 jsonb, column5 bool, column6 oid)")
    cursor.execute("insert into table1 (column1, column2, column3, column6) values (%s,%s,%s,%s)", (1,'Hello World!', 2, 777))
    cursor.execute("insert into table1 (column1, column2, column3) values (%s,%s,%s)", (2,'Hello World2!', 3))
    cursor.execute("insert into table2 values (%s,%s,%s)", ('Hello World3!', 4, 'Hello World4!'))
    connection.commit()
    connection.close()

    return lambda: con.connect()


@pytest.fixture
def connection_function_for_threadsafe_test(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("create table table1 (id serial primary key, created_date date not null default current_date, column1 integer default 1, column2 varchar(20), column3 integer, column4 float default 1.2, column5 bool default False, column6 oid default 999)")
    cursor.execute("create table table2 (id serial primary key, created_date date not null default current_date, column1 integer default 1, column2 varchar(20), column3 integer, column4 float default 1.2, column5 bool default False, column6 oid default 999)")

    for i in range(3000):
        cursor.execute("insert into table1 (column1, column2, column3, column6) values (%s,%s,%s,%s)", (i,'Hello World!', 2, 777))
        cursor.execute("insert into table2 (column1, column2, column3, column6) values (%s,%s,%s,%s)", (i, 'Hello World!', 2, 777))

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
        assert tables[0].column6 == 777

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

    def test_table_select(self, connection_function):
        mro.load_database(connection_function)
        assert len(mro.table1.select()) is 2
        assert len(mro.table1.select("column1=1")) is 1

    def test_table_select_pyformat_syntax(self, connection_function):
        mro.load_database(connection_function)

        initial_tables = mro.table1.select()
        injection_string = "1; insert into table1(column1, column2, column3) values(3,'Hello World3!',4); select * from table1"
        # Check we throw an exception if the input variable contains an injection string
        with pytest.raises(Exception):
            mro.table1.select("column1 = %s;", injection_string)

        # Check that since the attempted injection we haven't been able to insert another row using the select with user input
        current_tables = mro.table1.select()
        assert len(current_tables) == len(initial_tables)

        # Check the positive case, that we can select using pyformat syntax
        assert len(mro.table1.select("column1 = %s", 1)) is 1

    def test_table_select_count(self, connection_function):
        mro.load_database(connection_function)
        assert mro.table1.select_count() is 2
        assert mro.table1.select_count("column1=1") is 1

    def test_table_select_count_pyformat_syntax(self, connection_function):
        mro.load_database(connection_function)
        injection_string = "1; insert into table1(column1, column2, column3) values(3,'Hello World3!',4); select count(*) from table1"
        initial_table_count = mro.table1.select_count()

        with pytest.raises(Exception):
            mro.table1.select_count("column1 = %s;", injection_string)

        # Check that since the attempted injection we haven't been able to insert another row using the select count with user input
        current_table_count = mro.table1.select_count()
        assert current_table_count == initial_table_count

        # Check the positive case, that we can select count with pyformat syntax
        assert mro.table1.select_count("column1 = %s", 1) is 1

    def test_table_select_one(self, connection_function):
        mro.load_database(connection_function)
        assert mro.table1.select_one("column1 = 1").column1 is 1
        assert mro.table2.select_one().column2 is 4

    def test_table_select_one_pyformat_syntax(self, connection_function):
        mro.load_database(connection_function)
        injection_string = "1; insert into table1(column1, column2, column3) values(3,'Hello World3!',4); select * from table1"
        initial_table_count = mro.table1.select_count()

        with pytest.raises(Exception):
            mro.table1.select_one("column1 = %s;", injection_string)

        # Check that since the attempted injection we haven't been able to insert another row using the select count with user input
        current_table_count = mro.table1.select_count()
        assert current_table_count == initial_table_count

        # Check the positive case we can select one using this pyformat syntax
        assert mro.table1.select_one("column1 = %s", 1).column1 is 1

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

    def test_table_delete(self, connection_function):
        mro.load_database(connection_function)
        mro.table1.delete('column1 = 1')
        assert mro.table1.select_count('column1 = 1') is 0
        assert mro.table1.select_count() is not 0
        mro.table1.delete()
        assert mro.table1.select_count() is 0

    def test_table_delete_pyformat_syntax(self, connection_function):
        mro.load_database(connection_function)
        assert mro.table1.select_count("column1=1") is not 0

        mro.table1.delete('column2 = %s',
                          "1; insert into table1(column1,column2,column3) values(4, 'row in on delete', 6);")
        # Check we didn't delete as the column didn't match the whole string, also check we didn't insert a new row into the table
        assert mro.table1.select_count("column1 = 1") is not 0
        assert mro.table1.select_count("column2 = 'row in on delete'") is 0

        # Check the positive case, we can delete using the pyformat syntax
        mro.table1.delete("column1=%s",1)
        assert mro.table1.select_count("column1=1") is 0

    def test_create_object(self, connection_function):
        mro.load_database(connection_function)

        table_count = mro.table1.select_count()

        table = mro.table1(column1=3, column2='Hi!', column3=11, column6=10)

        assert table.column1 == 3
        assert table.column2 == 'Hi!'
        assert table.column3 == 11
        assert table.column6 == 10

        table = mro.table1(column2 = 'Hi2!')

        assert table.column1 == 1
        assert table.column2 == 'Hi2!'
        assert table.column3 is None

        kwargs = {'column1': 5, 'column2': 'Hi3!', 'column3': 78, 'column6': 22}
        table = mro.table1(**kwargs)

        assert table.column1 == 5
        assert table.column2 == 'Hi3!'
        assert table.column3 == 78
        assert table.column6 == 22

        tables = mro.table1.select()

        assert table_count + 3 == len(tables)

        assert tables[4].column1 == 5
        assert tables[4].column2 == 'Hi3!'
        assert tables[4].column3 == 78
        assert tables[4].column6 == 22

    def test_insert_check_default_values(self, connection_function):

        mro.load_database(connection_function)

        table_count = mro.table1.select_count()

        table = mro.table1(column1=3, column2='Hi!')
        assert table.column4 == 1.2
        assert table.column5 is False
        assert table.column6 == 999

        table = mro.table1(column1=3, column2='Hi!', column3=11, column4=5.7, column5=True, created_date=datetime.now().date(), column6=88)
        assert table.column4 == 5.7
        assert table.column5 is True
        assert table.column6 == 88

        tables = mro.table1.select()

        for table in tables:
            assert isinstance(table.id, int)
            assert table.id is not None
            assert isinstance(table.created_date, date)
            assert table.created_date is not None
            assert isinstance(table.column1, int)
            assert table.column1 is not None
            assert isinstance(table.column2, str)
            assert table.column2 is not None
            assert table.column3 is None or isinstance(table.column3, int)
            assert isinstance(table.column5, bool)
            assert isinstance(table.column6, int)

        table = mro.table3(column3='Hi56!', column4='{"data": 1}')

        table = mro.table3.select_one("column3 = 'Hi56!'")

        assert isinstance(table.column1, str)
        assert table.column1 == 'ABC DEF'
        assert isinstance(table.column3, str)
        assert table.column3 is not None
        assert isinstance(table.column4, MroDict)
        assert table.column4 is not None
        assert table.column5 is None
        assert table.column6 is None

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

    def test_disable_insert_thread_safe(self, connection_function_for_threadsafe_test):
        mro.load_database(connection_function_for_threadsafe_test)

        closedown_event = Event()

        thread1 = Thread(target=simple_select, args=(mro.table1.select, "thread1", closedown_event))
        thread1.start()

        thread2 = Thread(target=simple_select, args=(mro.table2.select, "thread2", closedown_event))
        thread2.start()

        thread3 = Thread(target=simple_select, args=(mro.table1.select, "thread3", closedown_event))
        thread3.start()

        thread1.join()
        thread2.join()
        thread3.join()

        successful = True
        if closedown_event.wait(0):
            successful = False

        assert successful


def simple_select(select_function, name, closedown_event):
    count = 0
    iterations = 10
    log_every = 3
    while count < iterations:
        try:
            if closedown_event.wait(0):
                return
            if count % log_every == 0:
                print(f"{name} Iterated {count} times")
            count = count + 1
            tables = select_function()
        except Exception as ex:
            print(f"Exception in {name}: {str(ex)}")
            closedown_event.set()
            return


if __name__ == '__main__':
    #pytest.main([__file__])
    pytest.main([__file__ + '::TestTable::test_insert_with_only_primary_key_no_kwargs'])