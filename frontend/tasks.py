import os
import uuid
import time
import json

from typing import List

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

from shared.storage import DiskStorage
from db.task import Task, DEFAULT_TASK_STORAGE
from shared.utils import load_jsonl

from .auth import login_required

bp = Blueprint('tasks', __name__, url_prefix='/tasks')

def load_annotation_requests(task_id, user_id): 
    task = Task.fetch(task_id)
    assert task is not None
    fname = os.path.join(DEFAULT_TASK_STORAGE, task_id, 'annotators', f'{user_id}.jsonl')
    data = load_jsonl(fname, to_df=False)
    return task, data

from joblib import Memory
location = './cachedir'
memory = Memory(location, verbose=0)
load_annotation_requests_cached = memory.cache(load_annotation_requests)

@bp.route('/<string:id>')
@login_required
def show(id):
    import time
    st = time.time()
    task, data = load_annotation_requests_cached(id, 'eddie')
    et = time.time()
    print(et-st)

    has_annotation = [False for _ in data]

    return render_template('tasks/show.html',
        task=task,
        data=data,
        has_annotation=has_annotation)

@bp.route('/<string:id>/annotate/<int:line>')
@login_required
def annotate(id, line):
    task, data = load_annotation_requests(id, 'eddie')

    return render_template('tasks/annotate.html',
        task=task,
        # You can pass more than one to render multiple examples
        data=json.dumps([data[line]]))

# from shared.storage import DiskStorage
# from shared.const import Const

# from prodigy_manager.prodigy_master import ProdigyMaster

# from .data_snapshot import (
#     create_labeled_data_snapshot,
#     get_labeled_data_snapshot,
#     DataSnapshotViewer
# )

# bp = Blueprint('tasks', __name__, url_prefix='/tasks')

# class Task:
#     def __init__(self, info):
#         self.info = info

#     def __str__(self):
#         return f"{self.info.get('name')}"

#     def get_created_at(self):
#         return self.info['created_at']

#     def get_id(self):
#         return self.info['task_id']

#     def get_name(self):
#         return self.info['name']

#     def get_data_fname(self):
#         return self.info['data_fname']
    
#     def get_labels(self):
#         labels = self.info['labels']
#         return labels.split(',')

#     def get_patterns_fname(self):
#         return self.info['patterns_fname']

#     def get_model_version(self):
#         return self.info.get('model', None)

#     def get_all_models(self):
#         models = [ModelViewer(self, v) for v in Const.get_all_model_versions(self.get_id())]
#         models = sorted(models, key=lambda x: x.get_order(), reverse=True)
#         return models

# def get_tasks():
#     storage = DiskStorage()
#     tasks = storage.read_all(prefix='tasks:')
#     tasks = [Task(x) for x in tasks]
#     tasks = sorted(tasks, key=lambda x: x.get_created_at(), reverse=True)
#     return tasks

# def get_task(task_id):
#     storage = DiskStorage()
#     tasks = storage.read_all(prefix='tasks:' + task_id)
#     if len(tasks) == 0:
#         return None
#     else:
#         assert len(tasks) == 1
#         return Task(tasks[0])

# # -----------------------------------------------------------------------------

# # TODO this should act more like a private class to Task - should not let others instantiate this?
# class ModelViewer:
#     def __init__(self, task:Task, model_version:str):
#         self.task = task
#         self.model_version = model_version
#         self.data_snapshot: DataSnapshotViewer = get_labeled_data_snapshot(self.task.get_id(), self.model_version)

#     def get_version(self):
#         return self.model_version

#     def get_order(self):
#         try:
#             # TODO: case when self.model_version is the str 'None'
#             return int(self.model_version)
#         except:
#             return 0

#     def has_labeled_data_snapshot(self):
#         f = Const.get_labeled_data_snapshot_fname(self.task.get_id(), self.model_version)
#         return os.path.isfile(f)

#     def has_trained_model(self):
#         d = Const.get_model_output_dir(self.task.get_id(), self.model_version)
#         return os.path.isdir(d) and len(os.listdir(d)) > 0

#     def has_inference_output_for_task(self):
#         f = Const.get_inference_output_fname(self.task.get_id(), self.model_version, self.task.get_data_fname())
#         return os.path.isfile(f)

#     def is_ready(self):
#         return self.has_labeled_data_snapshot() and \
#             self.has_trained_model() and \
#             self.has_inference_output_for_task()

#     def get_histogram_url(self):
#         fname = Const.get_inference_output_fname(self.task.get_id(), self.model_version, self.task.get_data_fname())
#         # TODO this should not be hard-coded
#         # TODO this uses the unsecure way to serve local files (see __init__.py:get_file)
#         return "file?f=" + f"{fname}.probs_pos_class.histogram.png"

#     def get_metrics(self):
#         fname = Const.get_model_metrics_fname(self.task.get_id(), self.model_version)
#         if os.path.isfile(fname):
#             with open(fname, 'r') as f:
#                 return json.loads(f.read())
#         return None

# # -----------------------------------------------------------------------------

# # TODO insecure way to access local files
# from flask import request, send_file
# @bp.route('/file', methods=['GET'])
# def get_file():
#     '''
#     localhost:5000/tasks/file?f=/tmp/output.png
#     '''
#     path = request.args.get('f')
#     print("Send file:", path)
#     return send_file(path)

# @bp.route('/')
# def index():
#     tasks = get_tasks()

#     all_files_in_default_storage = os.listdir(Const.DEFAULT_FILE_STRORAGE)

#     return render_template('tasks/index.html',
#         tasks=tasks,
#         default_file_storage=Const.DEFAULT_FILE_STRORAGE,
#         all_files_in_default_storage=all_files_in_default_storage)

# def _clean(s):
#     '''
#     s: input string from a form
#     '''
#     s = s.strip()
#     if len(s) == 0:
#         return None
#     return s

# @bp.route('/create', methods=('POST',))
# def create():
#     name = _clean(request.form['name'])
#     data_fname = _clean(request.form['data_fname'])
#     labels = _clean(request.form['labels'])
#     patterns_fname = _clean(request.form['patterns_fname'])
#     error = None

#     try:
#         assert name, 'Name is required'
#         assert data_fname, 'Data Filename is required'
#         assert labels, 'Labels is required'
#     except Exception as e:
#         error = str(e)

#     data_fname = os.path.join(Const.DEFAULT_FILE_STRORAGE, data_fname)
#     if patterns_fname:
#         patterns_fname = os.path.join(Const.DEFAULT_FILE_STRORAGE, patterns_fname)
    
#     if error is not None:
#         flash(error)
#     else:
#         task_id = str(uuid.uuid4())

#         storage = DiskStorage()
#         storage.write(f'tasks:{task_id}', {
#             'task_id': task_id,
#             'created_at': int(time.time()),

#             'name': name,
#             'data_fname': data_fname,
#             'labels': labels,
#             'patterns_fname': patterns_fname,

#             'model': None,
#         })
#         return redirect(url_for('tasks.index'))

#     return render_template('tasks/index.html')

# @bp.route('/<string:id>')
# def show(id):
#     task = get_task(id)

#     pm = ProdigyMaster()
#     servers = pm.get_all_servers_for_task(task)
#     n_servers_alive = len([s for s in servers if s.is_alive()])

#     models: List[ModelViewer] = task.get_all_models()

#     return render_template('tasks/show.html',
#         task=task,
#         n_servers_alive=n_servers_alive,
#         servers=servers,
#         models=models)

# @bp.route('/<string:id>/new_server', methods=('POST',))
# def new_server(id):
#     task = get_task(id)
#     pm = ProdigyMaster()
#     _ = pm.new_server_from_task(task)
#     return redirect(url_for('tasks.show', id=task.get_id()))

# @bp.route('/<string:id>/servers/<string:server_id>/stop', methods=('POST',))
# def stop_server(id, server_id):
#     pm = ProdigyMaster()
#     pm.stop_server(server_id)

#     task = get_task(id)
#     return redirect(url_for('tasks.show', id=task.get_id()))

# @bp.route('/<string:id>/new_model', methods=('POST',))
# def new_model(id):
#     task = get_task(id)

#     # TODO Put this on Celery

#     # TODO better Model object management
#     model_version = str(int(time.time()))

#     result: DataSnapshotViewer = create_labeled_data_snapshot(task.get_id(), model_version)
#     print("Data exported to:", result.get_fname())

#     # ---

#     from train_server.train_celery import train_model
#     from train_server.train_config import TrainConfig

#     config = TrainConfig(
#         data_snapshot_fname=result.get_fname(),
#         output_dir=Const.get_model_output_dir(task.get_id(), model_version),

#         task_id=task.get_id(),
#         model_version=model_version,

#         run_inference_on_fname=task.get_data_fname(),
#         inference_output_fname=Const.get_inference_output_fname(task.get_id(), model_version, task.get_data_fname())
#     )
#     train_model.delay(config)

#     # ---

#     return redirect(url_for('tasks.show', id=task.get_id()))

# @bp.route('/<string:id>/<string:model_version>/set_as_active_model', methods=('POST',))
# def set_as_active_model(id, model_version):
#     task = get_task(id)

#     task.info['model'] = model_version

#     storage = DiskStorage()
#     storage.write(f'tasks:{task.get_id()}', task.info)
    
#     return redirect(url_for('tasks.show', id=task.get_id()))

# # NOTE: We don't allow people to delete tasks for now.
# # @bp.route('/<string:id>/delete', methods=('POST',))
# # def delete(id):
# #     task = get_task(id)
# #     delete_task(task)
# #     return redirect(url_for('tasks.index'))