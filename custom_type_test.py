import pytest
import mro
import connection as con
from datetime import datetime, date


@pytest.fixture
def connection_function(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()
    cursor.execute("""DROP TYPE IF EXISTS custom_type""")
    cursor.execute("""DROP TYPE IF EXISTS custom_type2""")
    cursor.execute("""CREATE TYPE custom_type AS (custom_field_1 float,
                                                  custom_field_2 float,
                                                  custom_field_3 float,
                                                  custom_field_4 timestamp)""")
    cursor.execute("""CREATE TYPE custom_type2 AS (custom_field_1 bool,
                                                   custom_field_2 int)""")
    cursor.execute("create table table1 (id serial primary key,"
                                        "created_date date not null default current_date,"
                                        "column1 integer default 1,"
                                        "column2 varchar(20),"
                                        "column3 custom_type,"
                                        "column4 custom_type2)")
    cursor.execute("insert into table1 (column1, column2, column3, column4) values (%s,%s,%s, %s)", (1, 'Hello World!', (1.2345, 2.3456, 3.4567, datetime.now()), (False, 10)))
    cursor.execute("insert into table1 (column1, column2, column3, column4) values (%s,%s,%s, %s)", (2, 'Hello World2!', (12.345, 23.456, 34.567, datetime.now()), (True, 11)))
    cursor.execute("insert into table1 (column1, column2, column3, column4) values (%s,%s,%s, %s)", (3, 'Hello World3!', (12.345, None, 34.567, datetime.now()), (True, 11)))
    connection.commit()
    connection.close()

    return lambda: con.connect()


class TestTable(object):
    def test_table_reflection(self, connection_function):
        mro.load_database(connection_function)

        tables = mro.table1.select()

        assert len(tables) == 3

        assert isinstance(tables[0].created_date, date)
        assert tables[0].column1 == 1
        assert tables[0].column2 == 'Hello World!'
        assert tables[0].column3.custom_field_1 == 1.2345
        assert tables[0].column3.custom_field_2 == 2.3456
        assert tables[0].column3.custom_field_3 == 3.4567
        assert isinstance(tables[0].column3, mro.custom_types.custom_type)
        assert isinstance(tables[0].column4, mro.custom_types.custom_type2)

        assert tables[1].column1 == 2
        assert tables[1].column2 == 'Hello World2!'
        assert tables[1].column3.custom_field_1 == 12.345
        assert tables[1].column3.custom_field_2 == 23.456
        assert tables[1].column3.custom_field_3 == 34.567
        assert isinstance(tables[1].column3, mro.custom_types.custom_type)

        assert tables[2].column1 == 3
        assert tables[2].column2 == 'Hello World3!'
        assert tables[2].column3.custom_field_1 == 12.345
        assert tables[2].column3.custom_field_2 == None
        assert tables[2].column3.custom_field_3 == 34.567
        assert isinstance(tables[2].column3, mro.custom_types.custom_type)


    def test_table_select_filter(self, connection_function):
        mro.load_database(connection_function)

        tables = mro.table1.select('column1 = %d' % 2)

        assert len(tables) == 1

        assert tables[0].column1 == 2
        assert tables[0].column2 == 'Hello World2!'
        assert tables[0].column3.custom_field_1 == 12.345
        assert tables[0].column3.custom_field_2 == 23.456
        assert tables[0].column3.custom_field_3 == 34.567
        assert isinstance(tables[0].column3, mro.custom_types.custom_type)
        assert isinstance(tables[0].column4, mro.custom_types.custom_type2)

    def test_table_delete_filter(self, connection_function):
        mro.load_database(connection_function)

        table_count = mro.table1.select_count()
        tables = mro.table1.select('column1 = %d' % 2)

        assert len(tables) == 1

        assert tables[0].column1 == 2
        assert tables[0].column2 == 'Hello World2!'
        assert tables[0].column3.custom_field_1 == 12.345
        assert tables[0].column3.custom_field_2 == 23.456
        assert tables[0].column3.custom_field_3 == 34.567

        mro.table1.delete('column1 = %d' % 2)

        assert table_count - 1 == mro.table1.select_count()

    def test_create_object(self, connection_function):
        mro.load_database(connection_function)

        table_count = mro.table1.select_count()
        dt = datetime.now()
        table = mro.table1(column1=3, column2='Hi!', column3=mro.custom_types.custom_type(custom_field_1=1.2345,
                                                                                          custom_field_2=2.3456,
                                                                                          custom_field_3=3.4567,
                                                                                          custom_field_4=dt))

        assert table.column1 == 3
        assert table.column2 == 'Hi!'
        assert table.column3.custom_field_1 == 1.2345
        assert table.column3.custom_field_2 == 2.3456
        assert table.column3.custom_field_3 == 3.4567
        assert table.column3.custom_field_4 == dt

        table = mro.table1(column2='Hi2!')

        assert table.column1 == 1
        assert table.column2 == 'Hi2!'
        assert table.column3 == None

        kwargs = {'column1': 5, 'column2': 'Hi3!', 'column3': {"custom_field_1": 1.2345,
                                                               "custom_field_2": 2.3456,
                                                               "custom_field_3": 3.4567,
                                                               "custom_field_4": dt}}
        table = mro.table1(**kwargs)

        assert table.column1 == 5
        assert table.column2 == 'Hi3!'
        assert table.column3.custom_field_1 == 1.2345
        assert table.column3.custom_field_2 == 2.3456
        assert table.column3.custom_field_3 == 3.4567
        assert table.column3.custom_field_4 == dt

        tables = mro.table1.select()

        assert table_count + 3 ==len(tables)

        assert tables[5].column1 == 5
        assert tables[5].column2 == 'Hi3!'
        assert tables[5].column3.custom_field_1 == 1.2345
        assert tables[5].column3.custom_field_2 == 2.3456
        assert tables[5].column3.custom_field_3 == 3.4567
        assert tables[5].column3.custom_field_4 == dt

    def test_insert_check_default_values(self, connection_function):

        mro.load_database(connection_function)

        table = mro.table1(column1=3,
                           column2='Hi!',
                           column3=mro.custom_types.custom_type(custom_field_1=1.2345,
                                                                custom_field_2=2.3456,
                                                                custom_field_3=3.4567,
                                                                custom_field_4=datetime.now()),
                           created_date=datetime.now().date())

        tables = mro.table1.select()
        tables.append(table)
        for table in tables:
            assert isinstance(table.id, int)
            assert table.id != None
            assert isinstance(table.created_date, date)
            assert table.created_date != None
            assert isinstance(table.column1, int)
            assert table.column1 != None
            assert isinstance(table.column2, str)
            assert table.column2 != None
            assert table.column3 == None or isinstance(table.column3, mro.custom_types.custom_type)

    def test_insert_many(self, connection_function):
        mro.load_database(connection_function)

        mro.table1.delete()
        dt = datetime.now()
        table = mro.table1.insert_many(
            ['column1', 'column2', 'column3'],
            [
                [1, 'Hi!', mro.custom_types.custom_type(custom_field_1=10.2, custom_field_2=20.1, custom_field_3=30.4, custom_field_4=dt)],
                [2, 'Hi2!', mro.custom_types.custom_type(custom_field_1=30.3, custom_field_2=20.2, custom_field_3=10.1, custom_field_4=dt)],
                [3, 'Hi3!', mro.custom_types.custom_type(custom_field_1=40.1, custom_field_2=40.2, custom_field_3=40.3, custom_field_4=dt)]
            ])

        tables = mro.table1.select()

        assert 3 == len(tables)

        assert tables[0].column1 == 1
        assert tables[0].column2 == 'Hi!'
        assert tables[0].column3.custom_field_1 == 10.2
        assert tables[0].column3.custom_field_2 == 20.1
        assert tables[0].column3.custom_field_3 == 30.4
        assert tables[0].column3.custom_field_4 == dt

        assert tables[1].column1 == 2
        assert tables[1].column2 == 'Hi2!'
        assert tables[1].column3.custom_field_1 == 30.3
        assert tables[1].column3.custom_field_2 == 20.2
        assert tables[1].column3.custom_field_3 == 10.1
        assert tables[1].column3.custom_field_4 == dt

        assert tables[2].column1 == 3
        assert tables[2].column2 == 'Hi3!'
        assert tables[2].column3.custom_field_1 == 40.1
        assert tables[2].column3.custom_field_2 == 40.2
        assert tables[2].column3.custom_field_3 == 40.3
        assert tables[2].column3.custom_field_4 == dt

    def test_update_value(self, connection_function):
        mro.load_database(connection_function)

        table = mro.table1.select_one("column1 = 1")
        assert table.column3.custom_field_1 == 1.2345
        # Test we can assign a dictionary to the field
        table.column3 = {"custom_field_1": 10.2, "custom_field_2": 10.2, "custom_field_3": 10.2, "custom_field_4": datetime.now()}

        table = mro.table1.select_one("column1 = 1")
        # Check we updated the existing field and it was propagated to the db
        assert table.column3.custom_field_1 == 10.2

        # Check nothing else changed unexpectedly
        assert isinstance(table.created_date, date)
        assert table.column1 == 1
        assert table.column2 == 'Hello World!'
        table.column1 = 10

        with pytest.raises(NotImplementedError) as excinfo:
            table.column3.custom_field_1 = 1.5
        assert excinfo.value.args[0] == "You cannot set custom type internal attributes"

        assert len(mro.table1.select()) == 3



if __name__ == '__main__':
    #pytest.main([__file__])
    pytest.main([__file__ + '::TestTable::test_insert_with_only_primary_key_no_kwargs'])