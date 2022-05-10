# Potentially should be using UserDict here, however seems to result in a recursion issue in the init due to us
# overriding getattr
from copy import deepcopy

class MroDict(dict):
    def __init__(self, column, instance, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set the __instance on the dict like this to avoid calling the setattr of this class which would make it a key in the dictionary
        # Also name it __instance to try and avoid a clash with a key called instance
        object.__setattr__(self, '__instance', instance)
        object.__setattr__(self, '__column', column)

    def __deepcopy__(self, memodict):
        cls = self.__class__
        column = self._get_column()
        instance = self._get_instance()
        instance_id = id(instance)
        if instance_id in memodict:
            instance = memodict[instance_id]
        else:
            memodict[instance_id] = deepcopy(instance)
        new_copy = cls(column, instance, self.items())
        memodict[id(self)] = new_copy
        return new_copy

    def _get_instance(self):
        return self.__dict__['__instance']

    def _get_column(self):
        return self.__dict__['__column']

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value
        instance = self._get_instance()
        column = self._get_column()
        column.__set__(instance, instance.__getattribute__(column.name))

    def __delattr__(self, name):
        if name in self:
            del self[name]
            instance = self._get_instance()
            column = self._get_column()
            column.__set__(instance, instance.__getattribute__(column.name))
        else:
            raise AttributeError(name)
