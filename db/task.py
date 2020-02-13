import os
import uuid

from shared.utils import load_json, save_json, mkf, mkd

from inference.base import ITextCatModel
from inference.pattern_model import PatternModel
from inference.model_factory import ModelFactory

DEFAULT_DATA_STORAGE = '__data'
DEFAULT_TASK_STORAGE = '__tasks'
mkd(DEFAULT_DATA_STORAGE)
mkd(DEFAULT_TASK_STORAGE)

DIR_AREQ = 'ar' # Annotation Requests
DIR_ANNO = 'an' # Annotations

class Task:
    def __init__(self, name=None):
        self.task_id = str(uuid.uuid4())
        self.data_filenames = []
        self.models = []
        self.annotators = [] # A list of user_id's

        self.name = name
        if self.name is None:
            self.name = 'No Name'

    def __str__(self):
        return f'Task: {self.name}'

    def to_json(self):
        return {
            'name': self.name,
            'task_id': self.task_id,
            'data_filenames': self.data_filenames,
            'models': [m.to_json() for m in self.models],
            'annotators': self.annotators,
        }

    @staticmethod
    def from_json(data):
        task = Task(data.get('name'))
        task.task_id = data['task_id']
        task.data_filenames = data['data_filenames']
        task.models = [ModelFactory.from_json(m) for m in data['models']]
        task.annotators = data['annotators']
        return task

    # ------------------------------------------------------------

    def get_dir(self):
        '''Where files for this task are stored'''
        return os.path.join(DEFAULT_TASK_STORAGE, self.task_id)

    @staticmethod
    def fetch(task_id):
        task_config_path = os.path.join(DEFAULT_TASK_STORAGE, task_id, 'config.json')
        data = load_json(task_config_path)
        if data:
            return Task.from_json(data)
        else:
            return None

    def save(self):
        task_config_path = [DEFAULT_TASK_STORAGE, self.task_id, 'config.json']
        mkf(*task_config_path)
        task_config_path = os.path.join(*task_config_path)
        save_json(task_config_path, self.to_json())

    # ------------------------------------------------------------

    def add_data(self, fname:str):
        # TODO should only store fname, and access it via the "DEFAULT_DATA_STORAGE" as needed. True or False?
        self.data_filenames.append(
            os.path.join(DEFAULT_DATA_STORAGE, fname))

    def add_model(self, model:ITextCatModel):
        self.models.append(model)

    def add_annotator(self, user_id:str):
        self.annotators.append(user_id)
    
    def get_pattern_model(self):
        '''Return the first PatternModel, if one exists'''
        pattern_model = None
        for model in self.models:
            if isinstance(model, PatternModel):
                pattern_model = model
                break
        return pattern_model

def fetch_all_tasks():
    return os.listdir(DEFAULT_TASK_STORAGE)