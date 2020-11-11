import os
import re
import shutil
import uuid
from typing import List, Optional

from alchemy.db.model import TextClassificationModel
from alchemy.inference.nlp_model import NLPModel
from alchemy.shared.config import Config
from alchemy.shared.utils import (
    list_to_textarea,
    mkf,
    save_json,
    textarea_to_list,
)
from alchemy.shared.utils import mkd


def _convert_to_spacy_patterns(patterns: List[str]):
    return [
        {"label": "POSITIVE_CLASS", "pattern": [{"lower": x.lower()}]} for x in patterns
    ]


class _Task:
    def __init__(self, name=None):
        self.task_id = str(uuid.uuid4())
        self.annotators = []  # A list of user_id's
        self.labels = []
        self._data_filenames = []

        self.name = name
        if self.name is None:
            self.name = "No Name"

        # Access PatternModel via get_pattern_model,
        # after setting self.patterns_file first...
        # TODO deprecate patterns_file in favor or patterns.
        self.patterns_file = None
        self.patterns = []
        # This is a cache of the PatternModel instance.
        self._pattern_model = None

    def __str__(self):
        return self.name

    def get_clean_name(self):
        """Return a filesystem friendly (only alphanum chars) name."""
        return re.sub("[^0-9a-zA-Z]+", "_", self.name).lower()

    def to_json(self):
        return {
            "name": self.name,
            "task_id": self.task_id,
            "data_filenames": self._data_filenames,
            "annotators": self.annotators,
            "labels": self.labels,
            "patterns_file": self.patterns_file,
            "patterns": self.patterns,
        }

    @staticmethod
    def from_json(data):
        task = _Task(data.get("name"))
        task.task_id = data["task_id"]
        task._data_filenames = data["data_filenames"]
        task.annotators = data["annotators"]
        task.labels = data.get("labels", [])
        task.patterns_file = data.get("patterns_file", None)
        task.patterns = data.get("patterns", [])
        return task

    # ------------------------------------------------------------

    def get_dir(self):
        """Where files for this task are stored"""
        d = Config.get_tasks_dir()
        mkd(d)
        return os.path.join(d, self.task_id)

    def save(self):
        task_config_path = [self.get_dir(), "config.json"]
        mkf(*task_config_path)
        task_config_path = os.path.join(*task_config_path)
        save_json(task_config_path, self.to_json())

    def delete(self):
        """
        Use with caution! This deletes all the labels and models.
        """
        _dir = self.get_dir()
        if os.path.isdir(_dir):
            shutil.rmtree(_dir)

    # ------------------------------------------------------------

    def jinjafy(self, field):
        # A list of values, one on each line
        if field == "labels":
            return list_to_textarea(self.labels)
        if field == "annotators":
            return list_to_textarea(self.annotators)
        if field == "patterns":
            return list_to_textarea(self.patterns)
        return ""

    @staticmethod
    def parse_jinjafied(field, value):
        # A list of values, one on each line
        if field == "labels" or field == "annotators" or field == "patterns":
            return textarea_to_list(value)
        return None

    # ------------------------------------------------------------

    def get_model_viewers(self) -> List[TextClassificationModel]:
        raise Exception("Deprecated Function")

    def get_active_model_viewer(self) -> TextClassificationModel:
        raise Exception("Deprecated Function")

    def get_active_nlp_model(self) -> Optional[NLPModel]:
        raise Exception("Deprecated Function")
