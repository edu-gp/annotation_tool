import logging

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
    fetch_annotation_entity_and_ids_done_by_user_under_task)

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
        fetch_annotation_entity_and_ids_done_by_user_under_task(
            dbsession=db.session,
            username=username,
            labels=task.get_labels()
        )

    # TODO This may cause some duplication among the annotations and the
    #  requests. The first time this page loads, it will show the requests
    #  to be done and existing annotations. If the user annotates a request
    #  and refresh the page, one request will have the status of `Done` and
    #  annotations for that finished request will also pop up in the
    #  annotation section. Is this OK or do we want to guard against
    #  duplications based on the entity?

    et = time.time()
    print("Load time", et-st)

    return render_template('tasks/show.html',
                           task=task,
                           annotated=annotation_entity_and_ids_done_by_user_for_task,
                           ars=[item[0] for item in ar_id_and_status_pairs],
                           has_annotation=[item[1] ==
                                           AnnotationRequestStatus.Complete
                                           for item in ar_id_and_status_pairs])


@bp.route('/<string:task_id>/reannotate/<string:annotation_id>')
@login_required
def reannotate(task_id, annotation_id):
    username = g.user['username']
    user_id = fetch_user_id_by_username(db.session, username=username)

    task = db.session.query(Task).filter(
        Task.id == task_id).first()
    annotation_dict = construct_annotation_dict(db.session, annotation_id)

    logging.error(annotation_dict)

    next_annotation_id = get_next_annotation_id_from_db(
        dbsession=db.session,
        user_id=user_id,
        current_annotation_id=annotation_dict['annotation_id']
    )

    logging.error(next_annotation_id)

    annotations_on_entity_done_by_user = db.session.query(
        ClassificationAnnotation).filter(
        ClassificationAnnotation.entity == annotation_dict['entity'],
        ClassificationAnnotation.user_id == user_id).all()

    anno = build_empty_annotation(annotation_dict)
    for existing_annotation in annotations_on_entity_done_by_user:
        anno['anno']['labels'][
            existing_annotation.label] = existing_annotation.value

    anno['task_id'] = task.id
    anno['annotation_guides'] = {}
    anno['suggested_labels'] = task.get_labels()

    # Make sure the requested label is in the list.
    if annotation_dict['label'] not in anno['suggested_labels']:
        anno['suggested_labels'].insert(0, annotation_dict['label'])

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
    # we use this to differentiate between doing new annotations and
    # updating existing annotations
    anno['is_new_annotation'] = False
    # et = time.time()
    # print("Load time", et-st)

    # TODO more UI changes if we need to change the UI workflow for
    #  multi-labeling.

    return render_template('tasks/reannotate.html',
                           task=task,
                           anno=anno,
                           # You can pass more than one to render multiple examples
                           # TODO XXX left off here - make this work in the frontend
                           # 0. Create a test kitchen sink page.
                           # 1. Make sure the buttons remember state.
                           data=json.dumps([anno]),
                           next_ar_id=next_annotation_id)



@bp.route('/<string:task_id>/annotate/<string:ar_id>')
@login_required
def annotate(task_id, ar_id):
    username = g.user['username']
    user_id = fetch_user_id_by_username(db.session, username=username)

    task = db.session.query(Task).filter(
        Task.id == task_id).first()
    ar_dict = construct_ar_request_dict(db.session, ar_id)

    logging.error(ar_dict)

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

    # Fetch all existing annotations on this particular entity done by this
    # user regardless of the label.
    annotations_on_entity_done_by_user = db.session.query(ClassificationAnnotation).filter(
        ClassificationAnnotation.entity == ar_dict['entity'],
        ClassificationAnnotation.user_id == user_id).all()

    # Building the annotation request data for the suggested label
    anno = build_empty_annotation(ar_dict)
    for existing_annotation in annotations_on_entity_done_by_user:
        anno['anno']['labels'][existing_annotation.label] = existing_annotation.value

    anno['task_id'] = task.id
    anno['annotation_guides'] = {}
    anno['suggested_labels'] = task.get_labels()

    # Make sure the requested label is in the list.
    if ar_dict['label'] not in anno['suggested_labels']:
        anno['suggested_labels'].insert(0, ar_dict['label'])

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

    # we use this to differentiate between doing new annotations and
    # updating existing annotations
    anno['is_new_annotation'] = True

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

    context = {
        'data': data['req']['data'],
        'pattern_info': data['req']['pattern_info']
    }
    logging.error(context)
    logging.error("===============================")

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

    logging.error(annotation.id)
    logging.error("===============================")

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


@bp.route('/update_annotation', methods=['POST'])
@login_required
def update_annotation():
    '''API meant for Javascript to consume'''
    username = g.user['username']
    user_id = fetch_user_id_by_username(db.session, username=username)

    data = json.loads(request.data)
    logging.error(data)
    task_id = data['task_id']
    annotation_id = data['req']['annotation_id']

    # For all the labels received, we need to create/update annotations in
    # the db.
    annotation_result = data['anno']['labels']
    for label in annotation_result:
        value = annotation_result[label]
        annotation = get_or_create(dbsession=db.session,
                                   model=ClassificationAnnotation,
                                   id=annotation_id)
        annotation.value = value
        db.session.add(annotation)

    db.session.commit()

    next_annotation_id = get_next_annotation_id_from_db(
        dbsession=db.session,
        user_id=user_id,
        current_annotation_id=annotation_id
    )

    if next_annotation_id:
        return {'redirect': url_for('tasks.reannotate', task_id=task_id,
                                    annotation_id=next_annotation_id)}
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
