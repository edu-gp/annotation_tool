import logging

from ar.data import (
    fetch_all_ar, fetch_ar, fetch_annotation, fetch_all_ar_ids, get_next_ar,
    build_empty_annotation, annotate_ar,
    fetch_ar_ids, fetch_annotated_ar_ids_from_db, fetch_ar_by_id_from_db,
    get_next_ar_id_from_db, fetch_user_id_by_username,
    fetch_existing_classification_annotation_from_db, annotate_ar_in_db,
    fetch_ar_id_and_status)
import json
from flask import (
    Blueprint, g, render_template, request, url_for)

from db.model import db, AnnotationRequest, User, Task as NewTask, \
    get_or_create, ClassificationAnnotation, Label, update_instance, \
    AnnotationValue, AnnotationRequestStatus
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
    username = g.user['username']

    import time
    st = time.time()

    task = db.session.query(NewTask).filter(
        NewTask.id == id).first()
    ar_id_and_status_pairs = fetch_ar_id_and_status(dbsession=db.session,
                                                    task_id=id,
                                                    username=username)
    et = time.time()
    print("Load time", et-st)

    return render_template('tasks/show.html',
                           task=task,
                           ars=[item[0] for item in ar_id_and_status_pairs],
                           has_annotation=[item[1] ==
                                           AnnotationRequestStatus.Complete
                                           for item in ar_id_and_status_pairs])


@bp.route('/<string:task_id>/annotate/<string:ar_id>')
@login_required
def annotate(task_id, ar_id):
    username = g.user['username']
    user_id = fetch_user_id_by_username(db.session, username=username)

    task = db.session.query(NewTask).filter(
        NewTask.id == task_id).first()
    # TODO if we have ar_id, we don't need all 3 parameters to get the data
    ar_dict = fetch_ar_by_id_from_db(db.session, task_id, user_id, ar_id)
    next_ar_id = get_next_ar_id_from_db(
        dbsession=db.session,
        task_id=task_id,
        user_id=user_id,
        current_ar_id=ar_dict['ar_id']
    )

    # TODO could be optimized without this query as we only need the name
    #  later on.
    label = get_or_create(dbsession=db.session,
                          model=Label,
                          id=ar_dict['label_id'])

    # Fetch all existing annotations on this particular entity done by this
    # user regardless of the label.
    annotations_on_entity_done_by_user = \
        get_or_create(dbsession=db.session,
                      model=ClassificationAnnotation,
                      entity_id=ar_dict['entity_id'],
                      user_id=ar_dict['user_id'])

    # Building the annotation request data for the suggested label
    anno = build_empty_annotation(ar_dict)
    for existing_annotation in annotations_on_entity_done_by_user:
        # TODO label.name add an extra query to db.
        anno['anno']['labels'][existing_annotation.label.name] = \
            existing_annotation.value

    anno['suggested_labels'] = [label.name]
    anno['task_id'] = task.id

    # et = time.time()
    # print("Load time", et-st)

    # TODO more UI changes since we need to change the UI workflow.

    return render_template('tasks/annotate.html',
                           task=task,
                           anno=anno,
                           # You can pass more than one to render multiple examples
                           # TODO XXX left off here - make this work in the frontend
                           # 0. Create a test kitchen sink page.
                           # 1. Make sure the buttons remember state.
                           data=json.dumps([anno]),
                           next_ar_name=next_ar_id)


@bp.route('/receive_annotation', methods=['POST'])
@login_required
def receive_annotation():
    '''API meant for Javascript to consume'''
    username = g.user['username']
    user_id = fetch_user_id_by_username(db.session, username=username)[0]

    data = json.loads(request.data)
    task_id = data['task_id']
    ar_id = data['req']['ar_id']

    # TODO this is based on the assumption that there is only one label.
    anno_result = list(data['anno']['labels'].values())[0]
    annotation_id = data['req']['classification_annotation_id']

    annotate_ar_in_db(db.session, ar_id, annotation_id, anno_result)
    next_ar_name = get_next_ar_id_from_db(
        dbsession=db.session,
        task_id=task_id,
        user_id=user_id,
        current_ar_id=ar_id
    )[0]

    if next_ar_name:
        return {'redirect': url_for('tasks.annotate', task_id=task_id,
                                    ar_name=next_ar_name)}
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
