import logging

from ar.data import (
    build_empty_annotation, construct_ar_request_dict,
    get_next_ar_id_from_db, fetch_user_id_by_username,
    fetch_ar_id_and_status)
import json
from flask import (
    Blueprint, g, render_template, request, url_for)

from db.model import (
    db, AnnotationRequest, Task, get_or_create, ClassificationAnnotation,
    AnnotationValue, AnnotationRequestStatus, AnnotationGuide)

from .auth import login_required

bp = Blueprint('tasks', __name__, url_prefix='/tasks')


@bp.route('/<string:id>')
@login_required
def show(id):
    username = g.user['username']

    import time
    st = time.time()

    task = db.session.query(Task).filter(
        Task.id == id).first()
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

    task = db.session.query(Task).filter(
        Task.id == task_id).first()
    ar_dict = construct_ar_request_dict(db.session, ar_id)
    next_ar_id = get_next_ar_id_from_db(
        dbsession=db.session,
        task_id=task_id,
        user_id=user_id,
        current_ar_id=ar_dict['ar_id']
    )

    # TODO get_next_ar_id_from_db should just return the AnnotationRequest
    next_req = db.session.query(AnnotationRequest) \
        .filter_by(id=next_ar_id).one_or_none()
    # next_req could be None.
    label = ar_dict['label']

    # Fetch all existing annotations on this particular entity done by this
    # user regardless of the label.
    annotations_on_entity_done_by_user = db.session.query(ClassificationAnnotation).filter(
        ClassificationAnnotation.entity == ar_dict['entity'],
        ClassificationAnnotation.user_id == user_id).all()

    # Building the annotation request data for the suggested label
    anno = build_empty_annotation(ar_dict)
    for existing_annotation in annotations_on_entity_done_by_user:
        # TODO label.name add an extra query to db.
        anno['anno']['labels'][existing_annotation.label] = existing_annotation.value

    if label not in anno['anno']['labels']:
        anno['anno']['labels'][label] = AnnotationValue.NOT_ANNOTATED

    anno['suggested_labels'] = [label]
    anno['task_id'] = task.id

    guide = db.session.query(AnnotationGuide).filter_by(label=label).first()
    if guide:
        anno['annotation_guides'] = {
            label: {
                'text': guide.get_html()
            },
        }

    # et = time.time()
    # print("Load time", et-st)

    # TODO more UI changes if we need to change the UI workflow for
    #  multi-labeling.

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
    username = g.user['username']
    user_id = fetch_user_id_by_username(db.session, username=username)

    data = json.loads(request.data)
    task_id = data['task_id']
    ar_id = data['req']['ar_id']
    entity_type = data['req']['entity_type']
    entity = data['req']['entity']
    context = data['req']['data']['text']

    # For all the labels received, we need to create/update annotations in
    # the db.
    annotation_result = data['anno']['labels']
    for label in annotation_result:
        value = annotation_result[label]
        annotation = get_or_create(dbsession=db.session,
                                   model=ClassificationAnnotation,
                                   exclude_keys_in_retrieve=["context",
                                                             "value"],
                                   entity=entity,
                                   entity_type=entity_type,
                                   label=label,
                                   user_id=user_id,
                                   context=context,
                                   value=AnnotationValue.NOT_ANNOTATED)
        annotation.value = value
        db.session.add(annotation)

    # TODO only mark request as complete if the incoming label matches the
    # request label.
    annotatation_request = get_or_create(dbsession=db.session,
                                         model=AnnotationRequest,
                                         id=ar_id)
    annotatation_request.status = AnnotationRequestStatus.Complete
    db.session.add(annotatation_request)
    db.session.commit()

    next_ar_id = get_next_ar_id_from_db(
        dbsession=db.session,
        task_id=task_id,
        user_id=user_id,
        current_ar_id=ar_id
    )

    if next_ar_id:
        return {'redirect': url_for('tasks.annotate', task_id=task_id,
                                    ar_id=next_ar_id)}
    else:
        return {'redirect': url_for('tasks.show', id=task_id)}


@bp.route('/kitchen_sink')
# @login_required
def kitchen_sink():
    from db._task import _Task
    task = _Task()

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
        'annotation_guides': {
            'B2C': {
                'text': 'Consumer Company, excluding marketplace.'
            },
            'Healthcare': {
                'text': '<b>Anything</b> related to health.<br/>Does not include any related to fitness.'
            },
            'Fintech': {
                'text': '<i>Next generation</i> finance.'
            }
        }
    }

    anno_b = {
        'req': real_ar,
        'anno': {
            'labels': {}
        },
        'suggested_labels': ['Healthcare'],
        'task_id': task.task_id,
        'testing': True,
        'annotation_guides': {
            'B2C': {
                'text': 'Consumer Company, excluding marketplace.'
            },
            'Healthcare': {
                'text': '<b>Anything</b> related to health.<br/>Does not include any related to fitness.'
            },
            'Fintech': {
                'text': '<i>Next generation</i> finance.'
            }
        }
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
                           anno=anno_a,
                           task=task,
                           data=json.dumps([anno_a, anno_b, anno_c]),
                           next_ar_id='#')
