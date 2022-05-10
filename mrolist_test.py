import pytest
import mro
import connection as con
from copy import deepcopy
from mro.mro_list import MroList


@pytest.fixture(scope="module")
def connection(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("""create table test_type (id serial primary key,
                                              "json" json default '[1,2,3]');""")
    connection.commit()
    connection.close()

    mro.load_database(lambda: con.connect())

    return connection


class TestMroList(object):

    def test_mro_list_deepcopy(self, connection):
        obj = mro.test_type()
        assert type(obj.json) == MroList
        obj.json = [5,6,7]
        obj2 = deepcopy(obj)
        assert obj2.json == [5,6,7]
        assert obj2.json._get_column() == obj.json._get_column()
        assert id(obj.json._get_instance()) != id(obj2.json._get_instance())