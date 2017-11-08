import pytest
import mro
import connection as con
import psycopg2
from datetime import datetime, date, time
import uuid

xfail = pytest.mark.xfail


class test_type(object):

    varchar = mro.data_types.varchar('varchar', 0, 15, not_null=False, is_updatable=True, get_value_on_insert=False, is_primary_key=False)
    varchar2 = mro.data_types.varchar('varchar2', 1, 20, not_null=False, is_updatable=True, get_value_on_insert=False, is_primary_key=False)
    varchar_not_null = mro.data_types.varchar('varchar_not_null', 2, 15, not_null=True, is_updatable=True, get_value_on_insert=False, is_primary_key=False)
    varchar_not_updateble = mro.data_types.varchar('varchar_not_updateble', 3, 15, not_null=False, is_updatable=False, get_value_on_insert=False, is_primary_key=False)
    integer = mro.data_types.integer('integer', 4, not_null=False, is_updatable=True, get_value_on_insert=False, is_primary_key=False)
    boolean = mro.data_types.boolean('boolean', 5, not_null=False, is_updatable=True, get_value_on_insert=False, is_primary_key=False)


@pytest.fixture(scope="module")
def connection(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    # TODO make custom types work
    # cursor.execute("""
    # create type call_outcome AS ENUM ('No Answer', 'Answer Machine', 'Hung Up', 'Busy', 'Sale');""")

    cursor.execute("""create table test_type (
    id serial primary key,
    "varchar" varchar(15), 
    "varchar2" varchar(20), 
    "varchar_not_null" varchar(20) not null default 'abc',
    "integer" integer,
    "boolean" boolean,
    "time" time,
    "date" date,
    "timestamp" timestamp,
    "json" json,
    "jsonb" jsonb,
    "text" text default E'two\nlines',
    "double" double precision,
    "real" real,
    "uuid" uuid,
    "bytea" bytea);""")
    # "custom_enum" call_outcome);""")
    connection.commit()
    connection.close()

    mro.load_database(lambda: con.connect())

    return connection


class TestDataTypes(object):

    def test_varchar(self, connection):
        obj = mro.test_type(varchar = 'init')

        message = 'sldkhfaskjf ashdkfjahs dfkjashd'

        with pytest.raises(ValueError) as excinfo:
            obj.varchar = message
            message = 'Hey'
        assert excinfo.value.args[0] == 'Value length [{}] should not exceed [{}]'.format(len(message), 15)

        message = mro.test_type(varchar = 'init')

        with pytest.raises(TypeError) as excinfo:
            obj.varchar = message
        assert excinfo.value.args[0] == 'Value should be of type [str] not [{}]'.format(message.__class__ .__name__)

        message = 'Hello World!'

        obj.varchar = message

        assert obj.varchar == message

    def test_multi_object(self, connection):
        obj = mro.test_type(varchar = 'init')
        obj2 = mro.test_type(varchar = 'init')

        obj.varchar = '1'
        obj.varchar2 = '2'

        assert obj.varchar != obj.varchar2

        obj.varchar = '1'
        obj2.varchar = '2'

        assert obj.varchar != obj2.varchar

    def test_not_null(self, connection):
        obj = mro.test_type(varchar = 'init')

        assert obj.varchar_not_null == 'abc'
        with pytest.raises(ValueError) as excinfo:
            obj.varchar_not_null = None
        assert excinfo.value.args[0] == 'The value of [{}] cannot be null.'.format('varchar_not_null')

    @xfail
    def test_not_updateable(self, connection):
        raise Exception("Not implemented")
        obj = mro.test_type(varchar = 'init')

        obj.varchar = '1'
        assert obj.varchar == '1'
        with pytest.raises(PermissionError) as excinfo:
            obj.varchar_not_updateble = '2'
        assert excinfo.value.args[0] == 'The value of [{}] is not updateable.'.format('varchar_not_updateble')

    def test_integer(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.integer = 1
        assert obj.integer == 1
        with pytest.raises(TypeError) as excinfo:
            obj.integer = '1'
        assert excinfo.value.args[0] == 'Value should be of type [int] not [{}]'.format(str.__name__)

    def test_boolean(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.boolean = True
        assert obj.boolean == True
        with pytest.raises(TypeError) as excinfo:
            obj.boolean = 1
        assert excinfo.value.args[0] == 'Value should be of type [bool] not [{}]'.format(int.__name__)

    def test_time(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.time = time(17, 20)
        assert obj.time == time(17, 20)
        with pytest.raises(TypeError) as excinfo:
            obj.time = datetime(2015, 12, 21, 17, 20)
        assert excinfo.value.args[0] == 'Value should be of type [time] not [{}]'.format(datetime.__name__)

    def test_date(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.date = date(2015, 12, 21)
        assert obj.date == date(2015, 12, 21)
        with pytest.raises(TypeError) as excinfo:
            obj.date = datetime(2015, 12, 21, 17, 20)
        assert excinfo.value.args[0] == 'Value should be of type [date] not [{}]'.format(datetime.__name__)

    def test_datetime(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.timestamp = datetime(2015, 12, 21, 17, 20)
        assert obj.timestamp == datetime(2015, 12, 21, 17, 20)
        with pytest.raises(TypeError) as excinfo:
            obj.timestamp = date(2015, 12, 21)
        assert excinfo.value.args[0] == 'Value should be of type [datetime] not [{}]'.format(date.__name__)

    def test_json(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.json = '{"key": "value"}'
        assert obj.json == '{"key": "value"}'
        with pytest.raises(psycopg2.DataError) as excinfo:
            obj.json = 'this is just text'
        assert excinfo.value.args[0].startswith('invalid input syntax for type json')

    def test_jsonb(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.jsonb = '{"key": "value"}'
        assert obj.jsonb == '{"key": "value"}'
        with pytest.raises(psycopg2.DataError) as excinfo:
            obj.jsonb = 'this is just text'
        assert excinfo.value.args[0].startswith('invalid input syntax for type json')

    def test_text(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.text = '1'
        assert obj.text == '1'
        with pytest.raises(TypeError) as excinfo:
            obj.text = 1
        assert excinfo.value.args[0] == 'Value should be of type [str] not [{}]'.format(int.__name__)

    def test_double(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.double = 2.0
        assert obj.double == 2.0
        with pytest.raises(TypeError) as excinfo:
            obj.double = '1'
        assert excinfo.value.args[0] == 'Value should be of type [float] not [{}]'.format(str.__name__)

    def test_real(self, connection):
        obj = mro.test_type(varchar = 'init')

        obj.real = 2.0
        assert obj.real == 2.0
        with pytest.raises(TypeError) as excinfo:
            obj.real = '1'
        assert excinfo.value.args[0] == 'Value should be of type [float] not [{}]'.format(str.__name__)

    @xfail
    def test_uuid(self, connection):
        obj = mro.test_type(varchar = 'init')

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


if __name__ == '__main__':
    #pytest.main([__file__, '-rw'])
    pytest.main([__file__ + '::TestDataTypes::test_bytea'])