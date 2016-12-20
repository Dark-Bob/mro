import os
import pytest
import mro
import mro.foreign_keys
import connection as con

@pytest.fixture(scope="module")
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

class TestUpdates(object):

    def test_multiple_column_primary_key_update(self, connection):

        table = mro.table1(name='Test34', value='first')
        selectedTable = mro.table1.select_one("name='Test34'")
        assert selectedTable.value == 'first'
        table.value = 'second'
        selectedTable = mro.table1.select_one("name='Test34'")
        assert selectedTable.value == 'second'

    def test_update_fails_with_no_primary_key(self, connection):

        table = mro.table3(value='first')
        with pytest.raises(ValueError) as excinfo:
            table.value = 'second'
        assert excinfo.value.args[0] == "Update needs columns to match to update, is your table missing a prmary key?"

    def test_update_multiple_values(self, connection):

        table = mro.table1(name='Test35', value='first')
        assert mro.table1.select_count("name='Test35'") == 1
        assert mro.table1.select_count("name='Test36'") == 0
        table = table.update(name='Test36', value='second')
        assert table.name == 'Test36'
        assert table.value == 'second'
        assert mro.table1.select_count("name='Test35'") == 0
        assert mro.table1.select_count("name='Test36'") == 1
        selectedTable = mro.table1.select_one("name='Test36'")
        assert selectedTable.value == 'second'

    @pytest.mark.xfail
    def test_boolean(self, connection):

        table = mro.table4.insert(my_bool=True,my_boolean=False)

        table.my_bool = table.my_boolean

        assert table.my_bool is False
        assert table.my_boolean is False

if __name__ == '__main__':
    pytest.main([__file__])
    #pytest.main([__file__ + '::TestUpdates::test_update_multiple_values'])