import json
from uuid import UUID
from datetime import datetime
from mro.mro_dict import MroDict
from mro.mro_list import MroList


class JsonEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, MroList):
            return o.data
        elif isinstance(o, MroDict):
            return dict(o)
        if hasattr(o, '__dict__'):
            return o.__dict__

        return json.JSONEncoder.default(self, o)
