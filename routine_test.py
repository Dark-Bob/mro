import pytest
import json
import mro
import mro.helpers
import connection as con


def print_table(command, connection):
    cursor = connection.cursor()
    cursor.execute(command)
    cim = mro.helpers.create_column_name_index_map(cursor)
    print(cim)
    for row in cursor:
        print(row)


@pytest.fixture
def connection_function(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("create table table1 (created_datetime timestamp not null default current_timestamp, created_time time not null default current_time, column1 varchar(20) default 'ABC DEF', column2 integer, column3 jsonb, column4 bool)")
    cursor.execute("""
create procedure insert_row_table1(
    _column1 varchar(20) = 'default',
    _column2 integer = 0,
    _column3 jsonb = '{}'::jsonb,
    _column4 bool = true,
    _created_datetime timestamp = current_timestamp,
    _created_time time = current_time)
language sql
as $$
insert into table1 (created_datetime, created_time, column1, column2, column3, column4) values (_created_datetime, _created_time, _column1, _column2, _column3, _column4)
$$""")
    connection.commit()
    connection.close()

    return lambda: con.connect()


class TestRoutines(object):

    def test_in_parameters_with_defaults(self, connection_function):
        mro.load_database(connection_function)

        tables = mro.table1.select()

        assert len(tables) == 0

        mro.insert_row_table1()
        mro.insert_row_table1('test', 2, json.dumps({'some': 'thing'}), _column4=False)
        t = mro.table1.select()

        assert len(t) == 2

        assert t[0].column1 == 'default'
        assert t[0].column2 == 0
        assert t[0].column3 == {}
        assert t[0].column4 is True

        assert t[1].column1 == 'test'
        assert t[1].column2 == 2
        assert t[1].column3 == {'some': 'thing'}
        assert t[1].column4 is False

    def test_returns_table(self, connection_function):
        mro.connection.set_connection_function(connection_function)
        connection = mro.connection.connection
        cursor = connection.cursor()
        cursor.execute("""
create or replace function select_table1() returns table(col1 varchar(20), col2 integer)
language sql
immutable
as $$
select column1, column2 from table1
$$;""")
        connection.commit()

        mro.load_database(connection_function)

        mro.insert_row_table1()
        mro.insert_row_table1('test', 2, json.dumps({'some': 'thing'}), _column4=False)
        t = mro.select_table1()

        assert len(t) == 2

        assert t[0].col1 == 'default'
        assert t[0].col2 == 0

        assert t[1].col1 == 'test'
        assert t[1].col2 == 2

    def test_returns_scalar(self, connection_function):
        mro.connection.set_connection_function(connection_function)
        connection = mro.connection.connection
        cursor = connection.cursor()
        cursor.execute("""
create or replace function count_table1() returns integer
language sql
immutable
as $$
select count(column1) from table1
$$;""")
        connection.commit()

        mro.load_database(connection_function)

        mro.insert_row_table1()
        mro.insert_row_table1('test', 2, json.dumps({'some': 'thing'}), _column4=False)
        t = mro.table1.select()

        assert len(t) == mro.count_table1()

    def test_out_parameters(self, connection_function):
        mro.connection.set_connection_function(connection_function)
        connection = mro.connection.connection
        cursor = connection.cursor()
        cursor.execute("""
create or replace procedure one_two_three(in one integer, in two integer, inout three integer)
language plpgsql
as
$$
begin
three:=one+two;
end
$$""")
        connection.commit()
        mro.load_database(connection_function)

        result = mro.one_two_three(1, 2, None)

        assert result[0].three == 3