import pytest
import mro
import connection as con
from copy import deepcopy
from mro.mro_dict import MroDict

@pytest.fixture(scope="module")
def connection(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("""create table test_type (id serial primary key,
                                              "json" json default '{"key1": "value1"}');""")
    connection.commit()
    connection.close()

    mro.load_database(lambda: con.connect())

    return connection


class TestMroDict(object):

    def test_mro_dict_deepcopy(self, connection):
        obj = mro.test_type()
        assert type(obj.json) == MroDict
        obj.json = {"test3": 5}
        obj2 = deepcopy(obj)
        assert obj2.json.test3 == 5
        assert obj2.json._get_column() == obj.json._get_column()
        assert id(obj.json._get_instance()) != id(obj2.json._get_instance())