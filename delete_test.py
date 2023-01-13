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

    def test_delete(self, connection):

        new_to_delete_entry = mro.table1(name='Test34', value='exists')
        new_do_not_delete_entry = mro.table1(name='Test35', value='exists')
        table_entries_length = len(mro.table1.select())
        new_to_delete_entry = mro.table1.select_one("name='Test34'")
        assert new_to_delete_entry is not None
        new_to_delete_entry.delete()
        new_to_delete_entry = mro.table1.select_one("name='Test34'")
        assert new_to_delete_entry is None
        new_do_not_delete_entry = mro.table1.select_one("name='Test35'")
        assert new_do_not_delete_entry is not None
        assert len(mro.table1.select()) == table_entries_length - 1


if __name__ == '__main__':
    pytest.main([__file__])