import logging
from typing import Dict, Tuple

from ar.data import (
    build_empty_annotation, construct_ar_request_dict,
    get_next_ar_id_from_db, fetch_user_id_by_username,
    fetch_ar_id_and_status, construct_annotation_dict,
    get_next_annotation_id_from_db)
import json
from flask import (
    Blueprint, g, render_template, request, url_for)

from db.model import (
    db, AnnotationRequest, Task, get_or_create, ClassificationAnnotation,
    AnnotationGuide, AnnotationValue, AnnotationRequestStatus,
    fetch_annotation_entity_and_ids_done_by_user_under_labels)

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

    annotation_entity_and_ids_done_by_user_for_task = \
        fetch_annotation_entity_and_ids_done_by_user_under_labels(
            dbsession=db.session,
            username=username,
            labels=task.get_labels()
        )

    et = time.time()
    print("Load time", et-st)

    return render_template('tasks/show.html',
                           task=task,
                           annotated=annotation_entity_and_ids_done_by_user_for_task,
                           ars=[item[0] for item in ar_id_and_status_pairs],
                           has_annotation=[item[1] ==
                                           AnnotationRequestStatus.Complete
                                           for item in ar_id_and_status_pairs])


@bp.route('/<string:task_id>/examine/<string:user_under_exam>')
@login_required
def examine(task_id, user_under_exam):
    # TODO we should add UserRole control. Right now I'm just assuming only
    #  the admin can see the backend control page.
    task = db.session.query(Task).filter(Task.id == task_id).first()
    annotation_entity_and_ids_done_by_user_for_task = \
        fetch_annotation_entity_and_ids_done_by_user_under_labels(
            dbsession=db.session,
            username=user_under_exam,
            labels=task.get_labels()
        )

    return render_template('tasks/examine.html',
                           task=task,
                           annotated=annotation_entity_and_ids_done_by_user_for_task,
                           user_under_exam=user_under_exam,
                           is_admin_correction=True)


@bp.route('/<string:task_id>/annotate/<string:ar_id>')
@login_required
def annotate(task_id, ar_id):
    task, anno, next_example_id = _prepare_annotation_common(
        task_id=task_id,
        example_id=ar_id,
        is_request=True,
        username=g.user['username']
    )

    return render_template('tasks/annotate.html',
                           task=task,
                           anno=anno,
                           data=json.dumps([anno]),
                           next_ar_id=next_example_id)


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

    context = {
        'data': data['req']['data'],
        'pattern_info': data['req']['pattern_info']
    }

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


@bp.route('/<string:task_id>/reannotate/<string:annotation_id>')
@login_required
def reannotate(task_id, annotation_id):
    username = request.args.get('username', default=g.user['username'],
                                type=str)
    is_admin_correction = request.args.get('is_admin_correction',
                                           default=False, type=bool)
    task, anno, next_example_id = _prepare_annotation_common(
        task_id=task_id,
        example_id=annotation_id,
        is_request=False,
        username=username
    )
    anno["is_admin_correction"] = is_admin_correction
    if is_admin_correction:
        anno["update_redirect_link"] = url_for('tasks.examine',
                                               task_id=task_id, user_under_exam=username)
    else:
        anno["update_redirect_link"] = url_for('tasks.show', id=task_id)

    return render_template('tasks/annotate.html',
                           task=task,
                           anno=anno,
                           data=json.dumps([anno]),
                           next_annotation_id=next_example_id)


@bp.route('/update_annotation', methods=['POST'])
@login_required
def update_annotation():
    '''API meant for Javascript to consume'''
    # TODO need to check if this is for admin correction flow.
    username = g.user['username']

    data = json.loads(request.data)

    annotation_id = data['req']['annotation_id']

    annotation_result = data['anno']['labels']
    for label in annotation_result:
        value = annotation_result[label]
        annotation = get_or_create(dbsession=db.session,
                                   model=ClassificationAnnotation,
                                   id=annotation_id)
        annotation.value = value
        db.session.add(annotation)

    db.session.commit()

    return {'redirect': data.get('update_redirect_link')}


def _prepare_annotation_common(task_id: int,
                               example_id: int,
                               username: str,
                               is_request: bool = True) -> \
        Tuple[Task, Dict, int]:
    user_id = fetch_user_id_by_username(db.session, username=username)
    task = db.session.query(Task).filter(
        Task.id == task_id).first()

    if is_request:
        example_dict = construct_ar_request_dict(db.session,
                                                 example_id)
        next_example_id = get_next_ar_id_from_db(
            dbsession=db.session,
            task_id=task_id,
            user_id=user_id,
            current_ar_id=example_dict['ar_id']
        )
    else:
        example_dict = construct_annotation_dict(db.session, example_id)
        next_example_id = None

    annotations_on_entity_done_by_user = db.session.query(
        ClassificationAnnotation).filter(
        ClassificationAnnotation.entity == example_dict['entity'],
        ClassificationAnnotation.user_id == user_id).all()

    # NOTE: hack!
    # backfill the domain and name, in case they'remissing
    meta_dict = {
        'name': example_dict['entity'],
        'domain': example_dict['entity']
    }
    if example_dict['data'].get('meta') is None:
        example_dict['data']['meta'] = meta_dict

    anno = build_empty_annotation(example_dict)
    for existing_annotation in annotations_on_entity_done_by_user:
        if existing_annotation.label in task.get_labels():
            anno['anno']['labels'][
                existing_annotation.label] = existing_annotation.value

    anno['task_id'] = task.id
    anno['annotation_guides'] = {}
    anno['suggested_labels'] = task.get_labels()
    anno['username'] = username

    # Make sure the requested label is in the list.
    if example_dict['label'] not in anno['suggested_labels']:
        anno['suggested_labels'].insert(0, example_dict['label'])

    for label in anno['suggested_labels']:
        if label not in anno['anno']['labels']:
            anno['anno']['labels'][label] = AnnotationValue.NOT_ANNOTATED

        # TODO optimize query
        guide = db.session.query(
            AnnotationGuide).filter_by(label=label).first()
        if guide:
            anno['annotation_guides'][label] = {
                'html': guide.get_html()
            }

    anno['is_new_annotation'] = bool(is_request)

    return task, anno, next_example_id


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
                'html': 'Consumer Company, excluding marketplace.'
            },
            'Healthcare': {
                'html': '<b>Anything</b> related to health.<br/>Does not include any related to fitness.'
            },
            'Fintech': {
                'html': '<i>Next generation</i> finance.'
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
                'html': 'Consumer Company, excluding marketplace.'
            },
            'Healthcare': {
                'html': '<b>Anything</b> related to health.<br/>Does not include any related to fitness.'
            },
            'Fintech': {
                'html': '<i>Next generation</i> finance.'
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
