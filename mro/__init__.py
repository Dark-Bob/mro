
import mro.connection
import mro.data_types
import mro.table
import mro.sqlite


def disconnect():
    mro.connection.disconnect()


def load_database(connection_function):

    mro.connection.set_connection_function(connection_function)
    connection = mro.connection.connection

    if connection.__class__.__module__ == 'sqlite3':
        tables = sqlite._load_sqllite_db(connection)
    else:
        tables = _load_standard_db(connection)

    _create_classes(tables)

def _load_standard_db(connection):
    
    # Create data type functions
    def defaultColumnToDataType(column, code_start, code_end):
        return code_start + code_end

    def varcharColumnToDataType(column, code_start, code_end):
        character_maximum_length = column[8]
        return '{0}{1}, {2}'.format(code_start, character_maximum_length, code_end)

    # Create transform functions
    def default_transform(column_default, data_type):
        if column_default.endswith('::' + data_type):
            column_default = '"""{}"""'.format(column_default[1:-(len(data_type)+3)])
        return column_default, False

    def integer_transform(column_default, data_type):
        if column_default.endswith('::regclass)'):
            return None, True
        return column_default, False

    def none_transform(column_default, data_type):
        return None, True

    cursor = connection.cursor()

    tables = {}
    type_map = {
        'character varying': ['varchar', varcharColumnToDataType, default_transform],
        'integer': ['integer', defaultColumnToDataType, integer_transform],
        'timestamp without time zone': ['timestamp', defaultColumnToDataType, none_transform],
        'time without time zone': ['time', defaultColumnToDataType, none_transform],
        'date': ['date', defaultColumnToDataType, none_transform],
        'boolean': ['boolean', defaultColumnToDataType, default_transform],
        # TODO make work: 'bool': ['boolean', defaultColumnToDataType, default_transform],
        'text': ['text', defaultColumnToDataType, default_transform],
        'double precision': ['double', defaultColumnToDataType, default_transform],
        'real': ['real', defaultColumnToDataType, default_transform],
        'json': ['json', defaultColumnToDataType, default_transform],
        'jsonb': ['json', defaultColumnToDataType, default_transform],
        'uuid': ['uuid', defaultColumnToDataType, default_transform],
        'bytea': ['bytea', defaultColumnToDataType, default_transform],
        }

    # Get tables
    cursor.execute("select * from information_schema.tables where table_schema='public';")
    connection.commit()

    for table in cursor:
        table_name = table[2]

        cursor2 = connection.cursor()

        # get foreign keys
        cursor2.execute("""select  
            kcu1.column_name as fk_column_name
            ,kcu2.table_name as referenced_table_name 
            ,kcu2.column_name as referenced_column_name 
        from information_schema.referential_constraints as rc 

        inner join information_schema.key_column_usage as kcu1 
            on kcu1.constraint_catalog = rc.constraint_catalog  
            and kcu1.constraint_schema = rc.constraint_schema 
            and kcu1.constraint_name = rc.constraint_name 
            and kcu1.table_name = '{}'

        inner join information_schema.key_column_usage as kcu2 
            on kcu2.constraint_catalog = rc.unique_constraint_catalog  
            and kcu2.constraint_schema = rc.unique_constraint_schema 
            and kcu2.constraint_name = rc.unique_constraint_name 
            and kcu2.ordinal_position = kcu1.ordinal_position;""".format(table_name))
        connection.commit()

        foreign_keys = {}
        for foreign_key in cursor2:
            foreign_keys[foreign_key[0]] = (foreign_key[1], foreign_key[2])

        # get foreign keys
        cursor2.execute("""select  
            kcu1.table_name as fk_table_name
            ,kcu1.column_name as fk_column_name
            ,kcu2.column_name as referenced_column_name 
        from information_schema.referential_constraints as rc 

        inner join information_schema.key_column_usage as kcu1 
            on kcu1.constraint_catalog = rc.constraint_catalog  
            and kcu1.constraint_schema = rc.constraint_schema 
            and kcu1.constraint_name = rc.constraint_name

        inner join information_schema.key_column_usage as kcu2 
            on kcu2.constraint_catalog = rc.unique_constraint_catalog  
            and kcu2.constraint_schema = rc.unique_constraint_schema 
            and kcu2.constraint_name = rc.unique_constraint_name 
            and kcu2.ordinal_position = kcu1.ordinal_position
            and kcu2.table_name = '{}';""".format(table_name))
        connection.commit()

        foreign_key_targets = []
        for foreign_key in cursor2:
            foreign_key_targets.append((foreign_key[0], foreign_key[1], foreign_key[2]))

        # Get primary keys
        cursor2.execute("""select column_name from information_schema.table_constraints tc
            join information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
            where constraint_type='PRIMARY KEY' and tc.table_name='{}'""".format(table_name))
        connection.commit()
        primary_key_columns = [row[0] for row in cursor2]

        # Get columns
        cursor2.execute("select * from information_schema.columns where table_name='" + table_name + "';")
        connection.commit()

        col_data = []

        for column in cursor2:
            column_name = column[3]
            data_type = type_map[column[7]]
            column_index = column[4]-1
            column_default = column[5]
            is_nullable = column[6] == 'YES'
            is_updateable = column[43] == 'YES'
            get_value_on_insert = False
            is_primary_key = column_name in primary_key_columns

            if column_default:
                column_default, get_value_on_insert = data_type[2](column_default, column[7])

            code_start = 'data_types.{}("{}", {}, '.format(data_type[0], column_name, column_index)
            code_end = 'not_null={0}, is_updatable={1}, get_value_on_insert={2}, is_primary_key={3})'.format(not is_nullable, is_updateable, get_value_on_insert, is_primary_key)

            code = data_type[1](column, code_start, code_end)
            if column_name in foreign_keys:
                foreign_key = foreign_keys[column_name]
                # passing class name as string for eval to get around creation order issues
                code = 'mro.foreign_keys.foreign_key_data_type("{}", {}, "mro.{}", "{}")'.format(column_name, code, foreign_key[0], foreign_key[1])
            col_data.append((code, column_name, column_default))            

        for foreign_key_target in foreign_key_targets:
            foreign_key_name = foreign_key_target[0] + 's'
            # if they happen to have a column the same name as the refernece list don't add it
            if foreign_key_name not in [c[1] for c in col_data]:
                code = 'mro.foreign_keys.foreign_key_reference("{}", "mro.{}", "{}")'.format(foreign_key_target[2], foreign_key_target[0], foreign_key_target[1])
                col_data.append((code, foreign_key_target[0] + 's', None))

        tables[table_name] = col_data

    return tables   

def _create_classes(tables):
    for table, columns in tables.items():
        code = 'class ' + table + '(mro.table.table):\n'
        for column_data in columns:
            code += '    ' + column_data[1] + '=' + column_data[0] + '\n'
        code += """    def __init__(self, **kwargs):\n"""
        for column_data in columns:
            code += "        self.__dict__['" + column_data[1] + "'] = " + str(column_data[2]) + "\n"
        code += """        for k, v in kwargs.items():
            if not hasattr(self, k):
                raise ValueError("{} does not have an attribute {}".format(self.__class__.__name__, k))
            self.__dict__[k] = v

        if not super()._disable_insert:
            obj = super().insert(**kwargs)
            for c in """ + table + """._get_value_on_insert_columns:
                self.__dict__[c] = obj.__dict__[c]
            
    def update(self, **kwargs):
        primary_key_columns = """ + table + """._primary_key_columns
        primary_key_column_values = [self.__dict__[c] for c in primary_key_columns]

        super().update(primary_key_columns, primary_key_column_values, **kwargs)

        mro.table.table._disable_insert = True
        for k, v in kwargs.items():
            self.__dict__[k] = v
        mro.table.table._disable_insert = False

        return self\n"""
        code += "{}._register()".format(table)
        exec(code)
        generated_class = eval(table)
        globals()[generated_class.__name__] = generated_class