from .pattern_model import PatternModel

class ModelFactory:
    def from_json(data):
        if data['type'] == 'PatternModel':
            return PatternModel.from_json(data)
        else:
            return None
