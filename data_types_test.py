import pytest
import mro
import connection as con
from datetime import datetime, date, time
import uuid
from mro.mro_dict import MroDict
from mro.mro_list import MroList

xfail = pytest.mark.xfail


@pytest.fixture(scope="module")
def connection(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()
    # TODO re-add once custom enum types are supported, currently only custom composite types are
    # cursor.execute("""DROP TYPE IF EXISTS call_outcome""")
    # cursor.execute("""CREATE TYPE call_outcome AS ENUM ('No Answer', 'Answer Machine', 'Hung Up', 'Busy', 'Sale')""")

    cursor.execute("""create table test_type (
    id serial primary key,
    "varchar" varchar(20), 
    "varchar2" varchar(30), 
    "varchar_not_null" varchar(20) not null default 'abc',
    "integer" integer,
    "boolean" boolean,
    "time" time,
    "date" date,
    "timestamp" timestamp,
    "json" json default '{"key1": "value1"}',
    "jsonb" jsonb default '{"key": "value"}',
    "text" text default E'two\nlines',
    "double" double precision,
    "real" real,
    "uuid" uuid,
    "bytea" bytea,
    "oid" oid);""")
    # "custom_enum" call_outcome);""")
    connection.commit()
    connection.close()

    mro.load_database(lambda: con.connect())

    return connection


class TestDataTypes(object):

    def test_varchar(self, connection):
        obj = mro.test_type(varchar='init')

        message = 'sldkhfaskjf ashdkfjahs dfkjashd'

        with pytest.raises(ValueError) as excinfo:
            obj.varchar = message
            message = 'Hey'
        assert excinfo.value.args[0] == 'Value length [{}] should not exceed [{}]'.format(len(message), 20)

        message = mro.test_type(varchar='init')

        with pytest.raises(TypeError) as excinfo:
            obj.varchar = message
        assert excinfo.value.args[0] == 'Value should be of type [str] not [{}]'.format(message.__class__ .__name__)

        message = 'Hello World!'

        obj.varchar = message

        assert obj.varchar == message

    def test_multi_object(self, connection):
        obj = mro.test_type(varchar='init')
        obj2 = mro.test_type(varchar='init')

        obj.varchar = '1'
        obj.varchar2 = '2'

        assert obj.varchar != obj.varchar2

        obj.varchar = '1'
        obj2.varchar = '2'

        assert obj.varchar != obj2.varchar

    def test_not_null(self, connection):
        obj = mro.test_type(varchar='init')

        assert obj.varchar_not_null == 'abc'
        with pytest.raises(ValueError) as excinfo:
            obj.varchar_not_null = None
        assert excinfo.value.args[0] == 'The value of [{}] cannot be null.'.format('varchar_not_null')

    @xfail
    def test_not_updateable(self, connection):
        raise Exception("Not implemented")
        obj = mro.test_type(varchar='init')

        obj.varchar = '1'
        assert obj.varchar == '1'
        with pytest.raises(PermissionError) as excinfo:
            obj.varchar_not_updateable = '2'
        assert excinfo.value.args[0] == 'The value of [{}] is not updateable.'.format('varchar_not_updateable')

    def test_integer(self, connection):
        obj = mro.test_type(varchar='init')

        obj.integer = 1
        assert obj.integer == 1
        with pytest.raises(TypeError) as excinfo:
            obj.integer = '1'
        assert excinfo.value.args[0] == 'Value should be of type [int] not [{}]'.format(str.__name__)

    def test_boolean(self, connection):
        obj = mro.test_type(varchar='init')

        obj.boolean = True
        assert obj.boolean == True
        with pytest.raises(TypeError) as excinfo:
            obj.boolean = 1
        assert excinfo.value.args[0] == 'Value should be of type [bool] not [{}]'.format(int.__name__)

    def test_time(self, connection):
        obj = mro.test_type(varchar='init')

        obj.time = time(17, 20)
        assert obj.time == time(17, 20)
        with pytest.raises(TypeError) as excinfo:
            obj.time = datetime(2015, 12, 21, 17, 20)
        assert excinfo.value.args[0] == 'Value should be of type [time] not [{}]'.format(datetime.__name__)

    def test_date(self, connection):
        obj = mro.test_type(varchar='init')

        obj.date = date(2015, 12, 21)
        assert obj.date == date(2015, 12, 21)
        with pytest.raises(TypeError) as excinfo:
            obj.date = datetime(2015, 12, 21, 17, 20)
        assert excinfo.value.args[0] == 'Value should be of type [date] not [{}]'.format(datetime.__name__)

    def test_datetime(self, connection):
        obj = mro.test_type(varchar='init')

        obj.timestamp = datetime(2015, 12, 21, 17, 20)
        assert obj.timestamp == datetime(2015, 12, 21, 17, 20)
        with pytest.raises(TypeError) as excinfo:
            obj.timestamp = date(2015, 12, 21)
        assert excinfo.value.args[0] == 'Value should be of type [datetime] not [{}]'.format(date.__name__)

    def test_json(self, connection):
        obj = mro.test_type(varchar='initjson')

        # Test the default values
        assert obj.json.key1 == "value1"
        assert obj.json == MroDict(obj.json._get_column(), obj.json._get_instance(), {"key1": "value1"})
        assert isinstance(obj.json, MroDict)

        # Test we can change the value of the top level jsonb object
        obj.json = '{"key2": "value2"}'
        assert obj.json.key2 == "value2"
        assert obj.json == MroDict(obj.json._get_column(), obj.json._get_instance(), {"key2": "value2"})
        assert isinstance(obj.json, MroDict)
        with pytest.raises(AttributeError) as excinfo:
            print(obj.json.key)
        assert excinfo.value.args[0].startswith("key")

        # Test with top level list as well as nested lists
        obj.json = '[{"key1": "value1"},{"key2":[5,6,7]}]'
        assert isinstance(obj.json, MroList)
        assert isinstance(obj.json[0], MroDict)
        assert obj.json[0].key1 == "value1"
        assert obj.json[1].key2 == MroList(obj.json._get_column(), obj.json._get_instance(), [5, 6, 7])

        # Test setting individual items
        obj.json[0].key1 = "newvalue"
        db_obj = mro.test_type.select_one("varchar='initjson'")
        assert db_obj.json[0].key1 == "newvalue"

        # Test updating items
        db_obj.update(json='{"key1":5}')
        column = db_obj.json._get_column()
        instance = db_obj.json._get_instance()
        db_obj = mro.test_type.select_one("varchar='initjson'")
        assert db_obj.json == MroDict(column, instance, {"key1": 5})
        db_obj.json.key1 = 6
        db_obj = mro.test_type.select_one("varchar='initjson'")
        assert db_obj.json.key1 == 6
        db_obj.update(json=MroDict(column, instance, {"key3": 3}))
        assert db_obj.json == MroDict(column, instance, {"key3": 3})
        db_obj = mro.test_type.select_one("varchar='initjson'")
        assert db_obj.json == MroDict(column, instance, {"key3": 3})


        # Test inserting items
        new_db_obj = mro.test_type.insert(varchar="newjson",
                                          json={"key1": 6})
        new_db_obj = mro.test_type.insert(varchar="newjson",
                                          json=[1,2,3])
        column = new_db_obj.json._get_column()
        instance = new_db_obj.json._get_instance()
        new_db_obj = mro.test_type.select_one("varchar='newjson'")
        assert new_db_obj.json == MroDict(column, instance, {"key1": 6})

        # Test selecting multiple
        all_test_objs = mro.test_type.select()
        assert all([type(obj.json) in [MroDict, MroList] for obj in all_test_objs])

        # Test we can update individual values in the json
        obj = mro.test_type(varchar='single_value')
        obj.json = "5"
        obj = mro.test_type.select_one("varchar='single_value'")
        assert obj.json == 5

        # Try with a string value that could be accidentally converted to an int
        obj.json = '"5"'
        obj = mro.test_type.select_one("varchar='single_value'")
        assert obj.json == "5"

        # Try deleting an item in the json
        mro.test_type(varchar='delete_value')
        obj = mro.test_type.select_one("varchar='delete_value'")
        del obj.json.key1
        assert obj.json == {}

        # Check with some nested values
        obj.json = '[{"key1": "value1"},{"key2":[5,6,7]}]'
        assert obj.json[0].key1 == "value1"
        # Delete the first key and check we now have a list with an empty dictionary
        del obj.json[0].key1
        assert obj.json[0] == {}
        # Refetch from the db and check its still empty
        obj = mro.test_type.select_one("varchar='delete_value'")
        assert obj.json[0] == {}

        # Delete the first item in the json list, the second key should then become the first item
        # Check both local object and if we refetch from db
        del obj.json[0]
        assert obj.json[0] == {"key2": [5, 6, 7]}
        obj = mro.test_type.select_one("varchar='delete_value'")
        assert obj.json[0] == {"key2": [5, 6, 7]}

        # Test we can't set it to invalid json
        with pytest.raises(ValueError) as excinfo:
            obj.json = 'this is just text'
        assert excinfo.value.args[0].startswith('Invalid input syntax for type json, provided input: this is just text')

    def test_jsonb(self, connection):
        obj = mro.test_type(varchar='initjsonb')

        # Test the default values
        assert obj.jsonb.key == "value"
        assert obj.jsonb == MroDict(obj.jsonb._get_column(), obj.jsonb._get_instance(), {"key": "value"})
        assert isinstance(obj.jsonb, MroDict)

        # Test we can change the value of the top level jsonb object
        obj.jsonb = '{"key2": "value2"}'
        assert obj.jsonb.key2 == "value2"
        assert obj.jsonb == MroDict(obj.jsonb._get_column(), obj.jsonb._get_instance(), {"key2": "value2"})
        assert isinstance(obj.jsonb, MroDict)
        with pytest.raises(AttributeError) as excinfo:
            print(obj.jsonb.key)
        assert excinfo.value.args[0].startswith("key")

        # Test with top level list as well as nested lists
        obj.jsonb = '[{"key1": "value1"},{"key2":[5,6,7]}]'
        assert isinstance(obj.jsonb, MroList)
        assert isinstance(obj.jsonb[0], MroDict)
        assert obj.jsonb[0].key1 == "value1"
        assert obj.jsonb[1].key2 == MroList(obj.jsonb._get_column(), obj.jsonb._get_instance(), [5, 6, 7])

        # Test setting individual items
        obj.jsonb[0].key1 = "newvalue"
        db_obj = mro.test_type.select_one("varchar='initjsonb'")
        assert db_obj.jsonb[0].key1 == "newvalue"

        # Test updating items
        db_obj.update(jsonb='{"key1":5}')
        column = db_obj.jsonb._get_column()
        instance = db_obj.jsonb._get_instance()
        db_obj = mro.test_type.select_one("varchar='initjsonb'")
        assert db_obj.jsonb == MroDict(column, instance, {"key1": 5})
        db_obj.jsonb.key1 = 6
        db_obj = mro.test_type.select_one("varchar='initjsonb'")
        assert db_obj.jsonb.key1 == 6
        db_obj.jsonb = MroDict(column, instance, {"key1": 5})
        assert db_obj.jsonb.key1 == 5
        db_obj = mro.test_type.select_one("varchar='initjsonb'")
        assert db_obj.jsonb.key1 == 5
        db_obj.jsonb.key1 = MroList(column, instance, [1,2,3])
        assert db_obj.jsonb.key1 == [1, 2, 3]
        db_obj = mro.test_type.select_one("varchar='initjsonb'")
        assert db_obj.jsonb.key1 == MroList(column, instance, [1, 2, 3])
        db_obj.jsonb = MroList(column, instance, [1,2,3])
        db_obj = mro.test_type.select_one("varchar='initjsonb'")
        assert db_obj.jsonb[0] == 1
        db_obj.jsonb[:] = [2, 3]
        assert db_obj.jsonb == [2, 3]
        db_obj = mro.test_type.select_one("varchar='initjsonb'")
        assert db_obj.jsonb == [2, 3]
        db_obj.jsonb = MroList(column, instance, [1, 2, 3, 4])
        db_obj.jsonb = db_obj.jsonb[::-1]
        assert db_obj.jsonb == [4, 3, 2, 1]
        db_obj = mro.test_type.select_one("varchar='initjsonb'")
        assert db_obj.jsonb == [4, 3, 2, 1]


        # Test inserting items
        new_db_obj = mro.test_type.insert(varchar="newjsonb",
                                          jsonb={"key1": 6})
        column = new_db_obj.jsonb._get_column()
        instance = new_db_obj.jsonb._get_instance()
        new_db_obj = mro.test_type.select_one("varchar='newjsonb'")
        assert new_db_obj.jsonb == MroDict(column, instance, {"key1": 6})

        # Test selecting multiple
        all_test_objs = mro.test_type.select()
        assert all([type(obj.jsonb) in [MroDict, MroList] for obj in all_test_objs])

        # Test we can update individual values in the json
        obj = mro.test_type(varchar='single_jsb_value')
        obj.jsonb = "5"
        obj = mro.test_type.select_one("varchar='single_jsb_value'")
        assert obj.jsonb == 5

        obj.jsonb = '"5"'
        obj = mro.test_type.select_one("varchar='single_jsb_value'")
        assert obj.jsonb == "5"

        # Try deleting an item in the json
        mro.test_type(varchar='delete_jsb_value')
        obj = mro.test_type.select_one("varchar='delete_jsb_value'")
        del obj.jsonb.key
        assert obj.jsonb == {}

        # Check with some nested values
        obj.jsonb = '[{"key1": "value1"},{"key2":[5,6,7]}]'
        assert obj.jsonb[0].key1 == "value1"
        # Delete the first key and check we now have a list with an empty dictionary
        del obj.jsonb[0].key1
        assert obj.jsonb[0] == {}
        # Refetch from the db and check its still empty
        obj = mro.test_type.select_one("varchar='delete_jsb_value'")
        assert obj.jsonb[0] == {}

        # Delete the first item in the json list, the second key should then become the first item
        # Check both local object and if we refetch from db
        del obj.jsonb[0]
        assert obj.jsonb[0] == {"key2": [5, 6, 7]}
        obj = mro.test_type.select_one("varchar='delete_jsb_value'")
        assert obj.jsonb[0] == {"key2": [5, 6, 7]}

        # Test we can't set it to invalid json
        with pytest.raises(ValueError) as excinfo:
            obj.jsonb = 'this is just text'
        assert excinfo.value.args[0].startswith('Invalid input syntax for type json, provided input: this is just text')


    def test_text(self, connection):
        obj = mro.test_type(varchar='init')

        obj.text = '1'
        assert obj.text == '1'
        with pytest.raises(TypeError) as excinfo:
            obj.text = 1
        assert excinfo.value.args[0] == 'Value should be of type [str] not [{}]'.format(int.__name__)

    def test_double(self, connection):
        obj = mro.test_type(varchar='init')

        obj.double = 2.0
        assert obj.double == 2.0
        with pytest.raises(TypeError) as excinfo:
            obj.double = '1'
        assert excinfo.value.args[0] == 'Value should be of type [float] not [{}]'.format(str.__name__)

    def test_real(self, connection):
        obj = mro.test_type(varchar='init')

        obj.real = 2.0
        assert obj.real == 2.0
        with pytest.raises(TypeError) as excinfo:
            obj.real = '1'
        assert excinfo.value.args[0] == 'Value should be of type [float] not [{}]'.format(str.__name__)

    @xfail
    def test_uuid(self, connection):
        obj = mro.test_type(varchar='init')

        obj.uuid = uuid.uuid4()
        assert obj.uuid == uuid.uuid4()
        with pytest.raises(TypeError) as excinfo:
            obj.uuid = 'fail'
        assert excinfo.value.args[0] == 'Value should be of type [uuid] not [{}]'.format(str.__name__)

    @xfail
    def test_custom_enum(self, connection):
        obj = mro.test_type(varchar='init')

        obj.custom_enum = 'Busy'
        assert obj.custom_enum == 'Busy'
        with pytest.raises(TypeError) as excinfo:
            obj.custom_enum = 'Not Valid'
        assert excinfo.value.args[0] == 'Value should be of type [custom_enum] not [{}]'.format(str.__name__)

    def test_bytea(self, connection):
        bytea = 'my byte array'.encode('utf-8')

        obj = mro.test_type(bytea=bytea)
        obj.bytea = bytea
        assert obj.bytea == bytea
        with pytest.raises(TypeError) as excinfo:
            obj.bytea = 'Not Valid'
        assert excinfo.value.args[0] == 'Value should be of type [bytes] not [{}]'.format(str.__name__)

    def test_oid(self, connection):
        obj = mro.test_type(varchar='init')

        obj.oid = 1000
        assert obj.oid == 1000
        with pytest.raises(TypeError) as excinfo:
            obj.oid = 'randomstring'
        assert excinfo.value.args[0] == 'Value should be of type [int] not [{}]'.format(str.__name__)


if __name__ == '__main__':
    #pytest.main([__file__, '-rw'])
    pytest.main([__file__ + '::TestDataTypes::test_bytea'])