import collections


class InvalidRequest:
    def __init__(self):
        self.errors = []

    def add_error(self, parameter, message):
        self.errors.append({"parameter": parameter, "message": message})

    def has_errors(self):
        return len(self.errors) > 0

    def __bool__(self):
        return False


class ValidRequest:
    @classmethod
    def from_dict(cls, dict_data):
        raise NotImplementedError

    def __bool__(self):
        return True
