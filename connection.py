
import os
import psycopg2
import mro.helpers

global get_connection

if os.path.isfile('my_connection.py'):
    import my_connection
    get_connection = my_connection.get_connection
else:
    get_connection = lambda: psycopg2.connect(database='circle_test', user='ubuntu', password="secure_password")

def connect():
    global connection
    connection = get_connection()
    return connection

def disconnect():
    connection.close()

def drop_tables():
    # clear tables
    cursor = connection.cursor()
    cursor.execute("select * from information_schema.tables where table_schema='public';")
    connection.commit()

    for table in cursor:
        cursor2 = connection.cursor()
        cursor2.execute("drop table " + table[2] + " cascade;")
    connection.commit()

    # clear stored procs and functions
    cursor.execute("select * from information_schema.routines where routine_schema = 'public'")
    connection.commit()

    column_name_index_map = mro.helpers.create_column_name_index_map(cursor)
    for routine in cursor:
        cursor2 = connection.cursor()
        cursor2.execute(f"drop {routine[column_name_index_map['routine_type']]} {routine[column_name_index_map['routine_name']]}")
    connection.commit()