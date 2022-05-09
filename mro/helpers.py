import json

from .json_encoder import get_json_encoder_class
from .mro_list import MroList
from .mro_dict import MroDict


def create_column_name_index_map(cursor):
    description = cursor.description
    return {column.name: index for column, index in zip(description, range(len(description)))}


def mro_objects_to_json(obj):
    json_string = json.dumps(obj, cls=get_json_encoder_class())
    return json_string


def parse_to_mro_objects(column, instance, data):
    # Haven't seen a way to override the top level object being an array rather than an object
    if isinstance(data, list):
        data = list_to_mro_list(column, instance, data)
    elif isinstance(data, dict):
        data = dict_to_mro_dict(column, instance, data)
    return data


def dict_to_mro_dict(column, instance, dict_):
    output_dict = {}
    for k, v in dict_.items():
        if isinstance(v, list):
            output_dict[k] = list_to_mro_list(column, instance, v)
        elif isinstance(v, dict):
            output_dict[k] = dict_to_mro_dict(column, instance, v)
        else:
            output_dict[k] = v
    return MroDict(column, instance, output_dict)


def list_to_mro_list(column, instance, list_):
    output_list = []
    for item in list_:
        if isinstance(item, list):
            output_list.append(list_to_mro_list(column, instance, item))
        elif isinstance(item, dict):
            output_list.append(dict_to_mro_dict(column, instance, item))
        else:
            output_list.append(item)

    return MroList(column, instance, output_list)
