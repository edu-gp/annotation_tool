from typing import List, Optional
import os
import shutil
import uuid

from shared.utils import load_json, save_json, mkf, mkd

from inference.base import ITextCatModel
from inference.pattern_model import PatternModel

from db import _data_dir, _task_dir

from train.model_viewer import ModelViewer
from inference.nlp_model import NLPModel

DIR_AREQ = 'ar' # Annotation Requests
DIR_ANNO = 'an' # Annotations

class Task:
    def __init__(self, name=None):
        self.task_id = str(uuid.uuid4())
        self.annotators = [] # A list of user_id's
        self.labels = []
        self._data_filenames = []

        self.name = name
        if self.name is None:
            self.name = 'No Name'

        # Set self.patterns_file first, then call get_pattern_model
        self.patterns_file = None
        self._pattern_model = None

    def __str__(self):
        return self.name

    def to_json(self):
        return {
            'name': self.name,
            'task_id': self.task_id,
            'data_filenames': self._data_filenames,
            'annotators': self.annotators,
            'labels': self.labels,
            'patterns_file': self.patterns_file,
        }

    @staticmethod
    def from_json(data):
        task = Task(data.get('name'))
        task.task_id = data['task_id']
        task._data_filenames = data['data_filenames']
        task.annotators = data['annotators']
        task.labels = data.get('labels', [])
        task.patterns_file = data.get('patterns_file', None)
        return task

    # ------------------------------------------------------------

    def get_dir(self):
        '''Where files for this task are stored'''
        return _task_dir(self.task_id)

    @staticmethod
    def fetch(task_id):
        task_config_path = os.path.join(_task_dir(task_id), 'config.json')
        data = load_json(task_config_path)
        if data:
            return Task.from_json(data)
        else:
            return None

    def save(self):
        task_config_path = [self.get_dir(), 'config.json']
        mkf(*task_config_path)
        task_config_path = os.path.join(*task_config_path)
        save_json(task_config_path, self.to_json())

    @staticmethod
    def fetch_all_tasks(id_only=False):
        task_ids:List[os.DirEntry] = sorted(
            os.scandir(_task_dir()),
            key=lambda d: d.stat().st_mtime,
            reverse=True)
        task_ids = [x.name for x in task_ids]
        if id_only:
            return task_ids
        tasks = [Task.fetch(task_id) for task_id in task_ids]
        return tasks

    def delete(self):
        '''
        Use with caution! This deletes all the labels and models.
        '''
        _dir = self.get_dir()
        if os.path.isdir(_dir):
            shutil.rmtree(_dir)

    # ------------------------------------------------------------

    def _add_data(self, fname:str):
        # TODO test
        assert fname in os.listdir(_data_dir()), f"{fname} is not in {_data_dir}"
        if fname not in self._data_filenames:
            self._data_filenames.append(fname)

    def _add_annotator(self, user_id:str):
        if user_id not in self.annotators:
            self.annotators.append(user_id)
    
    def _add_label(self, label:str):
        # label = label.upper() # TODO Do I need to enforce this?
        if label not in self.labels:
            self.labels.append(label)

    def get_pattern_model(self):
        '''Return a PatternModel, if it exists'''
        if self._pattern_model is None:
            if self.patterns_file is not None:
                self._pattern_model = PatternModel(self.task_id, self.patterns_file)
        return self._pattern_model

    def update_and_save(self,
            name=None,
            labels=None,
            patterns_file=None,
            annotators=None,
            data_files=None):

        if name:
            self.name = name.strip()

        if data_files:
            for data in data_files:
                self._add_data(data)
        
        if annotators:
            for anno in annotators:
                self._add_annotator(anno)

        if labels:
            for label in labels:
                self._add_label(label)

        if patterns_file is not None:
            # TODO: Note this style means there's no way to delete the patterns model
            self.patterns_file = patterns_file
        
        self.save()
        return self

    # ------------------------------------------------------------
    
    def get_full_data_fnames(self):
        d = _data_dir()
        return [os.path.join(d, fname) for fname in self._data_filenames]

    def get_data_fnames(self):
        return self._data_filenames

    # ------------------------------------------------------------

    def jinjafy(self, field):
        # A list of values, one on each line
        if field == 'labels':
            return '\n'.join(self.labels)
        if field == 'annotators':
            return '\n'.join(self.annotators)
        return ''

    @staticmethod
    def parse_jinjafied(field, value):
        # A list of values, one on each line
        if field == 'labels' or field == 'annotators':
            res = [x.strip() for x in value.split('\n')]
            res = [x for x in res if len(x) > 0]
            return res
        return None

    # ------------------------------------------------------------

    def get_model_viewers(self) -> List[ModelViewer]:
        mvs = ModelViewer.fetch_all_for_task(self.task_id)
        return mvs[::-1] # Reverse list so latest is first

    def get_active_nlp_model(self) -> Optional[NLPModel]:
        # TODO logic to get active model will change.
        models = self.get_model_viewers()
        if len(models) > 0:
            version = models[0].version
            return NLPModel(self.task_id, version)
        else:
            return None
