from ar.data import (
    fetch_all_ar, fetch_ar, fetch_annotation, fetch_all_annotation_ids, get_next_ar,
    build_empty_annotation, annotate_ar
)
import json
from flask import (
    Blueprint, g, render_template, request, url_for
)

from db.task import Task

from .auth import login_required

bp = Blueprint('tasks', __name__, url_prefix='/tasks')

# TODO eddie update with new way of showing annotations
# TODO this has to be rewritten with

# def load_annotation_requests(task_id, user_id):
#     task = Task.fetch(task_id)
#     assert task is not None
#     fname = os.path.join(_task_dir(task_id), 'annotators', f'{user_id}.jsonl')
#     data = load_jsonl(fname, to_df=False)
#     return task, data

# from joblib import Memory
# location = './cachedir'
# memory = Memory(location, verbose=0)
# load_annotation_requests_cached = memory.cache(load_annotation_requests)


@bp.route('/<string:id>')
@login_required
def show(id):
    user_id = g.user['username']

    import time
    st = time.time()

    task = Task.fetch(id)
    ars = fetch_all_ar(id, user_id)
    annotated = set(fetch_all_annotation_ids(id, user_id))
    has_annotation = [x in annotated for x in ars]

    et = time.time()
    print("Load time", et-st)

    return render_template('tasks/show.html',
                           task=task,
                           ars=ars,
                           has_annotation=has_annotation)


@bp.route('/<string:id>/annotate/<string:ar_id>')
@login_required
def annotate(id, ar_id):
    user_id = g.user['username']

    # import time
    # st = time.time()

    task = Task.fetch(id)
    ar = fetch_ar(id, user_id, ar_id)
    next_ar_id = get_next_ar(id, user_id, ar_id)

    anno = fetch_annotation(id, user_id, ar_id)

    if anno is None:
        anno = build_empty_annotation(ar)

    anno['suggested_labels'] = task.labels
    anno['task_id'] = task.task_id

    # et = time.time()
    # print("Load time", et-st)

    return render_template('tasks/annotate.html',
                           task=task,
                           anno=anno,
                           # You can pass more than one to render multiple examples
                           # TODO XXX left off here - make this work in the frontend
                           # 0. Create a test kitchen sink page.
                           # 1. Make sure the buttons remember state.
                           data=json.dumps([anno]),
                           next_ar_id=next_ar_id)


@bp.route('/receive_annotation', methods=['POST'])
@login_required
def receive_annotation():
    '''API meant for Javascript to consume'''
    user_id = g.user['username']

    data = json.loads(request.data)

    task_id = data['task_id']
    ar_id = data['req']['ar_id']
    anno = data['anno']

    annotate_ar(task_id, user_id, ar_id, anno)

    next_ar_id = get_next_ar(task_id, user_id, ar_id)

    if next_ar_id:
        return {'redirect': url_for('tasks.annotate', id=task_id, ar_id=next_ar_id)}
    else:
        return {'redirect': url_for('tasks.show', id=task_id)}


@bp.route('/kitchen_sink')
# @login_required
def kitchen_sink():
    task = Task()

    simple_ar = {
        "ar_id": "059429e8faee44e2aabfeb0baaf5d44eb770c5fc9ff88043c9ff191f",
        "fname": "__data/spring_jan_2020_small.jsonl",
        "line_number": 963,
        "score": 0.01,
        "data": {
            "text": "Provider of a precise patient management platform intended to transform multiple clinical decision support workflows with relevant patient information. The company's platform is focused on delivering automatic tumor stagin, RT dose overlays, specialized patient-centric reporting, incidental findings management, clinical trials process streamlining and precision medicine initiatives, enabling physicians to treat their patients in an enhanced way.",
            "meta": {
                "name": "HealthMyne",
                "domain": "healthmyne.com"
            }
        }
    }

    real_ar = {
        "ar_id": "059429e8faee44e2aabfeb0baaf5d44eb770c5fc9ff88043c9ff191f",
        "fname": "__data/spring_jan_2020_small.jsonl",
        "line_number": 963,
        "score": 0.07575757575757576,
        "data": {
            "text": "Provider of a precise patient management platform intended to transform multiple clinical decision support workflows with relevant patient information. The company's platform is focused on delivering automatic tumor stagin, RT dose overlays, specialized patient-centric reporting, incidental findings management, clinical trials process streamlining and precision medicine initiatives, enabling physicians to treat their patients in an enhanced way.",
            "meta": {
                "name": "HealthMyne",
                "domain": "healthmyne.com"
            }
        },
        "pattern_info": {
            "tokens": [
                "Provider", "of", "a", "precise", "patient", "management", "platform", "intended", "to", "transform", "multiple", "clinical",
                "decision", "support", "workflows", "with", "relevant", "patient", "information", ".", "The", "company",
                "'s", "platform", "is", "focused", "on", "delivering", "automatic", "tumor", "stagin", ",", "RT", "dose", "overlays", ",",
                "specialized", "patient", "-", "centric", "reporting", ",", "incidental", "findings", "management", ",", "clinical", "trials",
                "process", "streamlining", "and", "precision", "medicine", "initiatives", ",", "enabling", "physicians", "to", "treat",
                "their", "patients", "in", "an", "enhanced", "way", "."
            ],
            "matches": [
                [4, 5, "patient"],
                [11, 12, "clinical"],
                [17, 18, "patient"],
                [37, 38, "patient"],
                [46, 47, "clinical"]
            ],
            "score": 0.07575757575757576
        }
    }

    anno_a = {
        'req': real_ar,
        'anno': {
            'labels': {
                'Healthcare': 1,
                'Fintech': -1,
            }
        },
        'suggested_labels': ['B2C', 'Healthcare', 'Fintech'],
        'task_id': task.task_id,
        'testing': True,
    }

    anno_b = {
        'req': real_ar,
        'anno': {
            'labels': {}
        },
        'suggested_labels': ['Healthcare'],
        'task_id': task.task_id,
        'testing': True,
    }

    anno_c = {
        'req': simple_ar,
        'anno': {
            'labels': {}
        },
        'suggested_labels': ['Healthcare'],
        'task_id': task.task_id,
        'testing': True,
    }

    # TODO what if an existing label does not exist in 'suggested_labels'? Do not show it? Undefined behavior?

    return render_template('tasks/annotate.html',
                           task=task,
                           data=json.dumps([anno_a, anno_b, anno_c]),
                           next_ar_id='#')
