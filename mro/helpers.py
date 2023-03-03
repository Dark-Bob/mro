
def create_column_name_index_map(cursor):
    description = cursor.description
    return {column.name: index for column, index in zip(description, range(len(description)))}