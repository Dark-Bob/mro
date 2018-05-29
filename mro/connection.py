
connection = None
connection_function = None


def set_connection_function(_connection_function):
    global connection
    global connection_function
    connection_function = _connection_function
    connection = connection_function()


def disconnect():
    global connection
    try:
        connection.close()
    except:
        pass


def reconnect():
    global connection
    global connection_function
    connection = connection_function()