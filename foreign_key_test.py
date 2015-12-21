import os
import pytest
import mro
import mro.foreign_keys
import connection as con

@pytest.fixture(scope="module")
def connection(request):
    connection = con.connect()
    request.addfinalizer(con.disconnect)

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

class TestForeignKeys(object):

    def test_read_foreign_key(self, connection):

        table = mro.table2.select_one('table1_id is not null')

        assert isinstance(table.table1_id.value, int)
        assert table.table1_id.value != None
        assert isinstance(table.table1_id.object, mro.table1)
        assert table.table1_id.object != None
        # check the _i matches up for both tables
        assert table.name.startswith(table.table1_id.object.name.replace("table1", "table2"))

    def test_null_foreign_key(self, connection):
        
        table = mro.table2.select_one('table1_id is null')

        assert table.table1_id.value == None
        assert table.table1_id.object == None

    def test_read_foreign_keys_reverse(self, connection):
        
        name = None
        table2s = mro.table2.select()
        table1_refs = [str(x.table1_id.value) for x in table2s if x.table1_id.value is not None]
        table = mro.table1.select_one('id in (' + ','.join(table1_refs) + ')')

        assert table.name != None
        assert table.table2s != None
        assert len(table.table2s) > 1

        num_table2s = len(table.table2s)
        mro.table2(name = 'table2_added', table1_id = table.id)

        assert len(table.table2s) == num_table2s
        table.table2s() # updates the reference list
        assert len(table.table2s) == num_table2s + 1

        num_table2s = len(table.table2s)
        table2 = mro.table2(name = 'table2_added2', table1_id = None)
        assert len(table.table2s) == num_table2s

        with pytest.raises(PermissionError) as excinfo:
            table.table2s[0] = table2
        assert excinfo.value.args[0] == "Cannot set specific value on foreign key reference list."
        
        table.table2s.append(table2)

        assert len(table.table2s) == num_table2s + 1
        # make sure the change is reflected in the database
        table.table2s() # updates the reference list
        assert len(table.table2s) == num_table2s + 1

    def test_read_foreign_keys_reverse_no_data(self, connection):
        
        table2s = mro.table2.select()
        table1_refs = [str(x.table1_id.value) for x in table2s if x.table1_id.value is not None]
        table = mro.table1.select_one('id not in (' + ','.join(table1_refs) + ')')

        assert table.name != None
        table2s = table.table2s
        assert not table2s

    def test_insert_class_that_has_foreign_references(self, connection):
        mro.table1(name = 'Bob')
        table = mro.table3(name = 'Bob2')
        # test that it's a varchar not a foreign key reference
        table.table4s = 'test string'

    def test_write_foreign_keys(self, connection):
        table1 = mro.table1.select_one()
        table2sCount = len(table1.table2s)
        table2 = mro.table2(name = 'table2_added2', table1_id = None)
        table2.table1_id = table1
        assert table2.table1_id.value == table1.id
        assert table2sCount == len(table1.table2s)
        table1.table2s()
        assert table2sCount + 1 == len(table1.table2s)

if __name__ == '__main__':
    #pytest.main([__file__])
    pytest.main([__file__ + '::TestForeignKeys::test_write_foreign_keys'])