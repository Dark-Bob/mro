import os
import pytest
import mro
import mro.foreign_keys
import connection as con
xfail = pytest.mark.xfail

@pytest.fixture(scope="module")
def connection(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("""create table table1 (
    id serial, 
    name varchar(20) not null,
    primary key (id)
    );""")

    cursor.execute("""create table table2 (
    id serial, 
    name varchar(20) not null,
    table1_id integer,
    primary key (id),
    foreign key (table1_id) references table1(id)
    );""")

    cursor.execute("""create table table3 (
    id serial, 
    name varchar(20) not null,
    table4s varchar(20),
    primary key (id)
    );""")

    cursor.execute("""create table table4 (
    id serial, 
    name varchar(20) not null,
    table3_id integer,
    primary key (id),
    foreign key (table3_id) references table3(id)
    );""")

    connection.commit()

    mro.load_database(connection)

    create_test_data(connection)

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

class TestViews(object):

    @xfail
    def test_read_view(self, connection):
        raise Exception("Not implemented");

    @xfail
    def test_insert_view_fails(self, connection):
        raise Exception("Not implemented");

    @xfail
    def test_update_view_fails(self, connection):
        raise Exception("Not implemented");

if __name__ == '__main__':
    #pytest.main([__file__])
    pytest.main([__file__ + '::TestViews::test_read_view'])