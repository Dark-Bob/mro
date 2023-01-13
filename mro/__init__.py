import mro.connection
import mro.data_types
import mro.table
import mro.sqlite
import mro.custom_types
import mro.routine
import types


def disconnect():
    mro.connection.disconnect()


def load_database(connection_function, hooks=None):
    print("***********INITIALISING DATABASE************")
    mro.connection.set_connection_function(connection_function)
    mro.connection.set_on_reconnect(init_db)
    mro.connection.set_hooks(hooks)
    connection = mro.connection.connection
    init_db(connection)
    if hooks is not None:
        for hook in hooks:
            hook()


def init_db(connection):
    if connection.__class__.__module__ == 'sqlite3':
        tables = sqlite._load_sqllite_db(connection)
    else:
        tables = _load_standard_db(connection)

    _create_classes(tables)

    mro.routine._create_routines(connection)


def execute_sql(sql, values=None):
    return mro.table.table._execute_sql(sql, values)


def _load_standard_db(connection):
    print('Loading standard db')
    cursor = connection.cursor()

    tables = {}

    # Create any custom types
    print('Creating custom types')
    mro.custom_types.create_custom_types(connection)

    # Get tables
    print('Getting tables')
    cursor.execute("select * from information_schema.tables where table_schema='public';")
    connection.commit()

    for table in cursor:
        table_name = table[2]
        print(f'Getting info about table [{table_name}]')

        cursor2 = connection.cursor()

        # Get foreign keys (part 1)
        # https://dba.stackexchange.com/a/218969
        cursor2.execute(f"""
            select 
                 col.attname as fk_column_name
                ,ftbl.relname as referenced_table_name
                ,fcol.attname as referenced_column_name
            from pg_catalog.pg_constraint con
            join lateral unnest(con.conkey) with ordinality as u(attnum, attposition) on true
            join pg_class tbl on tbl.oid = con.conrelid
            join pg_attribute col on (col.attrelid = tbl.oid and col.attnum = u.attnum)
            join lateral unnest(con.confkey) with ordinality as fu(attnum, attposition) on true
            join pg_class ftbl on ftbl.oid = con.confrelid
            join pg_attribute fcol on (fcol.attrelid = ftbl.oid and fcol.attnum = fu.attnum)
            where 
                    con.conrelid = '{table_name}'::regclass 
                and con.contype = 'f';
        """)
        connection.commit()

        foreign_keys = {}
        for foreign_key in cursor2:
            foreign_keys[foreign_key[0]] = (foreign_key[1], foreign_key[2])

        # Get foreign keys (part 2)
        # https://dba.stackexchange.com/a/218969
        cursor2.execute(f"""
            select 
                 tbl.relname
                ,col.attname
                ,fcol.attname
            from pg_catalog.pg_constraint con
            join lateral unnest(con.conkey) with ordinality as u(attnum, attposition) on true
            join pg_class tbl on tbl.oid = con.conrelid
            join pg_attribute col on (col.attrelid = tbl.oid and col.attnum = u.attnum)
            join lateral unnest(con.confkey) with ordinality as fu(attnum, attposition) on true
            join pg_class ftbl on ftbl.oid = con.confrelid
            join pg_attribute fcol on (fcol.attrelid = ftbl.oid and fcol.attnum = fu.attnum)
            where 
                    con.confrelid = '{table_name}'::regclass 
                and con.contype = 'f';        
        """)
        connection.commit()

        foreign_key_targets = []
        for foreign_key in cursor2:
            foreign_key_targets.append((foreign_key[0], foreign_key[1], foreign_key[2]))

        # Get primary keys
        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        cursor2.execute(f"""
            select
                a.attname
            from pg_index i
            join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
            where  
                    i.indrelid = '{table_name}'::regclass
                and i.indisprimary;        
        """)
        connection.commit()
        primary_key_columns = [row[0] for row in cursor2]

        # Get columns
        cursor2.execute(f"""
            select
                 column_name
                ,data_type
                ,udt_name
                ,ordinal_position
                ,column_default
                ,is_nullable
                ,is_updatable
                ,character_maximum_length
            from information_schema.columns 
            where 
                    table_name='{table_name}';
            """)
        connection.commit()

        columns = []
        for column in cursor2:
            col_data = {}
            column_name = column[0]
            postgres_type = column[1]
            if postgres_type == 'USER-DEFINED':
                postgres_type = column[2]
                data_type = mro.data_types.type_map[postgres_type]
                col_data['custom_type'] = eval(f'mro.custom_types.{postgres_type}')
            else:
                data_type = mro.data_types.type_map[postgres_type]
            column_index = column[3]-1
            column_default = column[4]
            is_nullable = column[5] == 'YES'
            is_updateable = column[6] == 'YES'
            get_value_on_insert = False
            is_primary_key = column_name in primary_key_columns

            if column_default:
                column_default, get_value_on_insert = data_type[2](column_default, postgres_type)

            col_data['data_type'] = data_type[0]
            col_data['column_name'] = column_name
            col_data['column_index'] = column_index
            col_data['column_default'] = column_default
            col_data['not_null'] = not is_nullable
            col_data['is_updateable'] = is_updateable
            col_data['get_value_on_insert'] = get_value_on_insert
            col_data['is_primary_key'] = is_primary_key
            col_data['length'] = column[7]
            if column_name in foreign_keys:
                foreign_key = foreign_keys[column_name]
                col_data['foreign_key'] = foreign_key

            columns.append(col_data)
        tables[table_name] = {}
        tables[table_name]['columns'] = columns
        tables[table_name]['foreign_key_targets'] = foreign_key_targets

    return tables


def _create_classes(tables):
    for table_name, table_data in tables.items():
        table_columns = table_data['columns']
        foreign_key_targets = table_data['foreign_key_targets']

        def create_table_class(name, columns):
            def update_function(self, **kwargs):
                primary_key_columns = self.__class__._primary_key_columns
                primary_key_column_values = [self.__dict__[c] for c in primary_key_columns]

                super(self.__class__, self).update(primary_key_columns, primary_key_column_values, **kwargs)

                with mro.table.disable_insert():
                    for k, v in kwargs.items():
                        self.__dict__[k] = v
                    return self

            def delete_function(self):
                primary_key_columns = self.__class__._primary_key_columns
                primary_key_column_values = [self.__dict__[c] for c in primary_key_columns]
                clause = " and ".join([c + '=%s' for c in primary_key_columns])
                super(self.__class__, self).delete(clause, *primary_key_column_values)

            def init_function(self, **kwargs):
                for column in columns:
                    self.__dict__[column['column_name']] = column['column_default']
                    custom_type = column.get('custom_type')
                    kwarg_for_column = kwargs.get(column['column_name'])
                    if kwarg_for_column is not None:
                        if custom_type is not None and type(kwarg_for_column) is not custom_type:
                            kwargs[column['column_name']] = custom_type(**kwarg_for_column)
                for k, v in kwargs.items():
                    if not hasattr(self, k):
                        raise ValueError(f"{self.__class__.__name__} does not have an attribute {k}")
                    self.__dict__[k] = v

                if not super(self.__class__, self)._insert.disabled:
                    obj = super(self.__class__, self).insert(**kwargs)
                    for c in self.__class__._get_value_on_insert_columns:
                        self.__dict__[c] = obj.__dict__[c]

                # Overriding the table wide update and delete methods so we can continue to use table methods on class objects.
                # The MethodType use makes sure the function gets the object instance as argument when called.
                self.update = types.MethodType(update_function, self)
                self.delete = types.MethodType(delete_function, self)

            attrib_dict = {'__init__': init_function}
            table_class = type(name, (mro.table.table,), attrib_dict)
            return table_class

        dynamic_table_class = create_table_class(table_name, table_columns)

        for column in table_columns:
            kwargs = {"name": column['column_name'],
                      "column_index": column['column_index'],
                      "not_null": column['not_null'],
                      "is_updateable": column['is_updateable'],
                      "get_value_on_insert": column['get_value_on_insert'],
                      "is_primary_key": column['is_primary_key']}
            if column['data_type'] == 'varchar':
                kwargs['length'] = column['length']
            if column.get('custom_type') is not None:
                kwargs['python_type'] = column['custom_type']

            col_value = mro.data_types.__dict__[column['data_type']](**kwargs)

            # Add attributes to class
            setattr(dynamic_table_class, column['column_name'], col_value)
            # Add foreign key attributes to the class
            if column.get('foreign_key') is not None:
                setattr(dynamic_table_class,
                        column['column_name'],
                        mro.foreign_keys.foreign_key_data_type(column['column_name'],
                                                               col_value,
                                                               f'mro.{column["foreign_key"][0]}',
                                                               column["foreign_key"][1]))

        for foreign_key_target in foreign_key_targets:
            foreign_key_name = f"{foreign_key_target[0]}s"
            # if they happen to have a column the same name as the reference list don't add it
            if foreign_key_name not in [column['column_name'] for column in table_columns]:
                setattr(dynamic_table_class,
                        foreign_key_name,
                        mro.foreign_keys.foreign_key_reference(foreign_key_target[2],
                                                               f"mro.{foreign_key_target[0]}",
                                                               foreign_key_target[1]))

        setattr(mro, dynamic_table_class.__name__, dynamic_table_class)
        dynamic_table_class._register()
