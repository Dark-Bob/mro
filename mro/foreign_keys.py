
import mro
import json

class foreign_key_data_type(object):

    def __init__(self, name, data_type, reference_class, reference_column_name):
        self.name = name + '_foreign_key'
        self.data_type = data_type
        self.reference_class = reference_class
        self.reference_column_name = reference_column_name

    def __get__(self, instance, instance_type):
        if instance is None:
            return self
        elif not hasattr(instance, self.name):
            self._lazy_init(instance)
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        if not hasattr(instance, self.name):
            self._lazy_init(instance)

        if isinstance(value, self.reference_class):
            instance.__dict__[self.name].value = value.__dict__[self.reference_column_name]
        else:
            instance.__dict__[self.name].value = value

    def _lazy_init(self, instance):
        # lazy eval of reference class checked once per instance to get around class creation order issues
        if isinstance(self.reference_class, str):
            self.reference_class = eval(self.reference_class)
        instance.__dict__[self.name] = foreign_key(instance, self.data_type, self.reference_class, self.reference_column_name)


# Ensure foreign keys just get saved as the key value not the json object
class foreign_key_json_encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, foreign_key):
            return obj.value
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)

json._default_encoder = foreign_key_json_encoder()


class foreign_key(object):

    def __init__(self, owner, data_type, reference_class, reference_column_name):
        self.__dict__['data_type'] = data_type
        self.__dict__['reference_class'] = reference_class
        self.__dict__['reference_column_name'] = reference_column_name
        self.__dict__['owner'] = owner

    def __getattr__(self, attribute):
        value = self.data_type.__get__(self.owner, int)
        if attribute == 'value':
            return value
        elif attribute == 'object':
            if value == None:
                self.__dict__['object'] = None
            else:
                self.__dict__['object'] = self.reference_class.select_one("{} = {}".format(self.reference_column_name, value))
            return self.__dict__['object']
        else:
            raise AttributeError("Attribute [{}] does not exist.".format(attribute))

    def __setattr__(self, attribute, value):
        if attribute == 'value':
            self.data_type.__set__(self.owner, value)
            if 'object' in self.__dict__:
                del self.__dict__['object']
        elif attribute == 'object':
            # we could allow this to be set and if the object is different update the reference accordingly
            raise PermissionError("Cannot set the object attribute directly.")
        else:
            raise AttributeError("Illegal attribute [{}] on this object.".format(attribute))

    def __repr__(self):
        value = self.data_type.__get__(self.owner, int)
        return str(value)

    def __eq__(self, other):
        value = self.data_type.__get__(self.owner, int)
        if type(other) is int:
            return value == other
        elif type(other) is foreign_key:
            return value == other.data_type.__get__(other.owner, int)
        else:
            return False


class foreign_key_reference(object):

    def __init__(self, target_column, referring_class, referring_column):
        self.name = referring_class + '_foreign_refs'
        self.target_column = target_column
        self.referring_class = referring_class
        self.referring_column = referring_column

    def __get__(self, instance, instance_type):
        if instance is None:
            return self
        
        if self.name in instance.__dict__:
            return instance.__dict__[self.name]

        if isinstance(self.referring_class, str):
            self.referring_class = eval(self.referring_class)

        instance.__dict__[self.name] = foreign_key_reference_list(instance, self.target_column, self.referring_class, self.referring_column)
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        raise Exception('Cannot set foreign key reference list, perhaps you meant to append to or extend the list?')


class foreign_key_reference_list(list):

    def __init__(self, target_instance, target_column, referring_class, referring_column):
        self.target_instance = target_instance
        self.target_column = target_column
        self.referring_class = referring_class
        self.referring_column = referring_column
        super().__init__(self)
        super().extend(self.referring_class.select(self.referring_column + '=' + str(getattr(self.target_instance, self.target_column))))

    def __getitem__(self, key):
        return super().__getitem__(key)

    def __setitem__(self, key, item):
        raise PermissionError("Cannot set specific value on foreign key reference list.")

    def __call__(self):
        super().clear()
        super().extend(self.referring_class.select(self.referring_column + '=' + str(getattr(self.target_instance, self.target_column))))

    def append(self, object):
        setattr(object, self.referring_column, getattr(self.target_instance, self.target_column))
        return super().append(object)