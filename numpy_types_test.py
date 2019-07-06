import pytest
import mro
import connection as con
import pandas as pd


@pytest.fixture
def connection_function(request):
    connection = con.connect()
    request.addfinalizer(mro.disconnect)

    cursor = connection.cursor()

    con.drop_tables()

    cursor.execute("create table table1 (id serial primary key, column1 integer default 1, column2 varchar(20), column3 float, column4 boolean)")
    connection.commit()
    connection.close()

    return lambda: con.connect()


def test_numpy_type_insertion_with_col_names(connection_function):
    mro.load_database(connection_function)
    df = pd.DataFrame(data={'column1': [1, 2, 3], 'column2': ['abc', 'bcd', 'cde'], 'column3': [0.0, 0.1, 0.2], 'column4': [True, False, True]})

    for i in range(df.shape[0]):
        d = {k: v for k, v in zip(df.columns, df.iloc[i].values)}
        mro.table1.insert(**d)


def test_numpy_type_update(connection_function):
    mro.load_database(connection_function)
    df = pd.DataFrame(data={'column1': [1, 2, 3], 'column2': ['abc', 'bcd', 'cde'], 'column3': [0.0, 0.1, 0.2], 'column4': [True, False, True]})

    for i in range(df.shape[0]):
        d = {k: v for k, v in zip(df.columns, df.iloc[i].values)}
        mro.table1.insert(**d)

    t = mro.table1.select_one()
    t.column1 = df.iloc[i].values[0]
    t.column2 = df.iloc[i].values[1]
    t.column3 = df.iloc[i].values[2]
    t.column4 = df.iloc[i].values[3]
