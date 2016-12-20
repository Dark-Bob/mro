
connection = None
connection_function = None


def set_connection_function(_connection_function):
    global connection
    global connection_function
    connection_function = _connection_function
    connection = connection_function()


def disconnect():
    connection.close()


def reconnect():
    global connection
    connection = connection_function()