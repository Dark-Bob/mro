
import sqlite3
import psycopg2

global get_connection

if False:
    import my_connection
    get_connection = my_connection.get_connection()
else:
    get_connection = lambda: psycopg2.connect(database='circle_test', user='ubuntu')

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

    # clear stored procs
    cursor.execute("select * from information_schema.routines where routine_type = 'PROCEDURE'")
    connection.commit()

    for proc in cursor:
        cursor2 = connection.cursor()

    # clear views