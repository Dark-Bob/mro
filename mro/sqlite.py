
import mro.data_types as data_types

def _load_sqllite_db(connection):
    cursor = connection.cursor()

    tables = {}
    # Get tables
    cursor.execute("select * from sqlite_master;")
    connection.commit()

    for table in cursor:
        # Get columns
        definition = table[4]
        index = definition.find('(')
        definition = definition[index+1:len(definition)-1]
        columns = definition.split(', ')
        col_dict = {}
        for column in columns:
            col_split = column.split(' ')
            col_dict[col_split[0]] = data_types.type_map[col_split[1]]

        tables[table[1]] = col_dict

    return tables