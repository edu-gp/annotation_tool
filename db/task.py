import os
import uuid

from shared.storage import DiskStorage
from shared.utils import load_jsonl, save_jsonl, mkf, mkd

from inference.base import ITextCatModel
from inference.model_factory import ModelFactory

DEFAULT_DATA_STORAGE = '__data'
DEFAULT_TASK_STORAGE = '__tasks'
mkd(DEFAULT_DATA_STORAGE)
mkd(DEFAULT_TASK_STORAGE)

DIR_ANNOTATORS = 'annotators'

class Task:
    def __init__(self):
        self.task_id = str(uuid.uuid4())
        self.data_filenames = []
        self.models = []

    def to_json(self):
        return {
            'task_id': self.task_id,
            'data_filenames': self.data_filenames,
            'models': [m.to_json() for m in self.models],
        }

    @staticmethod
    def from_json(data):
        task = Task()
        task.task_id = data['task_id']
        task.data_filenames = data['data_filenames']
        task.models = [ModelFactory.from_json(m) for m in data['models']]
        return task

    # ------------------------------------------------------------

    def get_dir(self):
        return os.path.join(DEFAULT_TASK_STORAGE, self.task_id)

    @staticmethod
    def fetch(task_id):
        db = DiskStorage()
        data = db.read(f'task:{task_id}')
        if data:
            return Task.from_json(data)
        else:
            return None

    def save(self):
        db = DiskStorage()
        db.write(f'task:{self.task_id}', self.to_json())

    # ------------------------------------------------------------

    def add_data(self, fname:str):
        self.data_filenames.append(
            os.path.join(DEFAULT_DATA_STORAGE, fname))

    def add_model(self, model:ITextCatModel):
        self.models.append(model)
