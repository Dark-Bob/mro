import atexit

connection = None
connection_function = None
reconnect_function = None
hooks = None


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


def set_on_reconnect(_reconnect_function):
    global reconnect_function
    reconnect_function = _reconnect_function


def set_hooks(_hooks):
    global hooks
    hooks = _hooks


def reconnect():
    global connection
    global connection_function
    global reconnect_function
    connection = connection_function()
    print("***********RECONNECTING DATABASE************")
    reconnect_function(connection)
    if hooks is not None:
        for hook in hooks:
            hook()


atexit.register(disconnect)