import json
from mro.mro_dict import MroDict
from mro.mro_list import MroList


class JsonEncoder(json.JSONEncoder):

    @classmethod
    def ObjectHandler(cls, parsedDict):
        return parsedDict

    def default(self, o):
        if isinstance(o, MroList):
            return list(MroList)
        elif isinstance(o, MroDict):
            return dict(o)
        if hasattr(o, '__dict__'):
            return o.__dict__
        return json.JSONEncoder.default(self, o)


class JsonEncoderWrapper(JsonEncoder):
    user_encoder = None

    @classmethod
    def ObjectHandler(cls, parsedDict):
        parsedDict = cls.user_encoder.ObjectHandler(parsedDict)
        parsedDict = super().ObjectHandler(parsedDict)
        return parsedDict

    def default(self, o):
        o = self.user_encoder().default(o)
        if isinstance(o, dict):
            return o
        o = super().default(o)
        return o

json_encoder_class = JsonEncoder


def get_json_encoder_class():
    global json_encoder_class
    return json_encoder_class


def set_user_encoder(user_encoder):
    global json_encoder_class
    JsonEncoderWrapper.user_encoder = user_encoder
    json_encoder_class = JsonEncoderWrapper

