import pytest
import mro
import mro.foreign_keys
import connection as con


@pytest.fixture()
def connection(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("""create table table1 (
    id serial unique, 
    name varchar(20) not null,
    value varchar(20),
    primary key (id, name)
    );""")

    cursor.execute("""create table table2 (
    id serial, 
    name varchar(20) not null,
    table1_id integer,
    primary key (id),
    foreign key (table1_id) references table1(id)
    );""")

    cursor.execute("""create table table3 (
    value varchar(20) not null
    );""")

    cursor.execute("""create table table4 (
    my_bool bool default False,
    my_boolean boolean default True
    );""")

    connection.commit()

    create_test_data(connection)

    connection.close()

    mro.load_database(lambda: con.connect())

    return connection


def create_test_data(connection):

    cursor = connection.cursor()
    num_table1 = 2
    for i in range(1,num_table1+1):
        cursor.execute("insert into table1 (name) values (%s)", ('table1_{}'.format(i),))
        for j in range(1,4):
            cursor.execute("insert into table2 (name, table1_id) values (%s,%s)", ('table2_{}_{}'.format(i, j), i))

    # edge cases
    cursor.execute("insert into table2 (name, table1_id) values (%s,%s)", ('table2_None', None))
    cursor.execute("insert into table1 (name) values (%s)", ('table1_None',))

    connection.commit()


def test_execute_sql(connection):

    mro.execute_sql('select * from table1')


def test_execute_sql_disconnected(connection):
    mro.disconnect()
    mro.execute_sql('select * from table1')


if __name__ == '__main__':
    pytest.main([__file__])
    #pytest.main([__file__ + '::test_update_multiple_values'])