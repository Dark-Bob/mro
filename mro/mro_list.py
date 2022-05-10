from copy import deepcopy

class MroList(list):
    def __init__(self, column, instance, initial_list):
        super().__init__(initial_list)
        self.__instance = instance
        self.__column = column

    def __deepcopy__(self, memodict):
        cls = self.__class__
        column = self._get_column()
        instance = self._get_instance()
        instance_id = id(instance)
        if instance_id in memodict:
            instance = memodict[instance_id]
        else:
            memodict[instance_id] = deepcopy(instance)
        new_copy = cls(column, instance, self)
        memodict[id(self)] = new_copy
        return new_copy

    def _get_instance(self):
        return self.__instance

    def _get_column(self):
        return self.__column

    def __setitem__(self, i, item):
        super().__setitem__(i, item)
        instance = self._get_instance()
        column = self._get_column()
        column.__set__(instance, instance.__getattribute__(column.name))

    def __delitem__(self, i):
        super().__delitem__(i)
        instance = self._get_instance()
        column = self._get_column()
        column.__set__(instance, instance.__getattribute__(column.name))

    def __getitem__(self, i):
        if isinstance(i, slice):
            return self.__class__(self._get_column(), self.__instance, super().__getitem__(i))
        else:
            return super().__getitem__(i)
