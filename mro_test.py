import os
import pytest
import psycopg2
import mro
import connection as con

@pytest.fixture(scope="module")
def connection(request):
    connection = con.connect()
    request.addfinalizer(con.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("create table table1 (id serial primary key, column1 integer, column2 varchar(20), column3 integer)")
    cursor.execute("create table table2 (id serial primary key, column1 varchar(20), column2 integer, column3 varchar(20))")
    cursor.execute("create table table3 (id serial primary key)")
    cursor.execute("insert into table1 (column1, column2, column3) values (%s,%s,%s)", (1,'Hellow World!', 2))
    cursor.execute("insert into table1 (column1, column2, column3) values (%s,%s,%s)", (2,'Hellow World2!', 3))
    cursor.execute("insert into table2 (column1, column2, column3) values (%s,%s,%s)", ('Hellow World3!', 4, 'Hellow World4!'))
    connection.commit()

    mro.load_database(connection)

    return connection

class TestMro(object):

    def test_table_reflection(self, connection):

        table1 = mro.table1.select_one()
        table2 = mro.table2.select_one()

        assert table1.__class__.__name__ == 'table1'
        assert table2.__class__.__name__ == 'table2'

        text = 'Hello World!'
        number = 1
        with pytest.raises(TypeError) as excinfo:
            table1.column1 = text
        assert excinfo.value.args[0] == 'Value should be of type [int] not [{}]'.format(text.__class__.__name__ )

        table1.column1 = number
        table1.column2 = text

        assert table1.column1 == number
        assert table1.column2 == text

        with pytest.raises(TypeError) as excinfo:
            table2.column1 = number
        assert excinfo.value.args[0] == 'Value should be of type [str] not [{}]'.format(number.__class__.__name__  )

        table2.column1 = text
        table2.column2 = number

        assert table2.column1 == text
        assert table2.column2 == number

    def test_recovery_from_failed_insert(self, connection):
        mro.table3.insert(id=1)

        with pytest.raises(psycopg2.IntegrityError) as e:
            mro.table3.insert(id=1)
        assert e.value.args[0].startswith('duplicate key value violates unique constraint')

        mro.table3.insert(id=2)

if __name__ == '__main__':
    pytest.main([__file__])