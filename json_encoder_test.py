import json

import pytest
from enum import Enum
import mro
from mro.data_types import MroDict
import connection as con


class CustomType(Enum):
    NUMERIC = 'numeric'
    TEXT = 'text'
    DATE = 'date'


class CustomJsonEncoder(json.JSONEncoder):
    @classmethod
    def ObjectHandler(cls, parsedDict):
        if '__class__' in parsedDict:
            if parsedDict['__class__'] == 'CustomType':
                return CustomType[parsedDict['__value__']]
        return parsedDict

    def default(self, o):
        if isinstance(o, CustomType):
            return {
                '__class__': 'CustomType',
                '__value__': o.name
            }

        if hasattr(o, '__dict__'):
            return o.__dict__

        return json.JSONEncoder.default(self, o)


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

    mro.load_database(lambda: con.connect(), json_encoder=CustomJsonEncoder)

    return connection


class TestJsonEncoder(object):

    def test_user_defined_encoder(self, connection):
        obj = mro.test_type(json='{"key2":"value2"}')

        obj2 = mro.test_type.select()[0]
        assert type(obj.json) == MroDict
        obj.json.key2 = CustomType.NUMERIC
        obj2 = mro.test_type.select()[0]
        assert type(obj2.json.key2) == CustomType