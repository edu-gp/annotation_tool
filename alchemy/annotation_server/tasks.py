import json
import logging
from typing import Dict, Tuple

from flask import Blueprint, g, render_template, request, url_for
from sqlalchemy.exc import DatabaseError
from werkzeug.urls import url_decode

from alchemy.ar.data import (
    _construct_comparison_df,
    build_empty_annotation,
    construct_annotation_dict,
    construct_ar_request_dict,
    count_ar_under_task_and_user,
    count_completed_ar_under_task_and_user,
    fetch_ar_id_and_status,
    fetch_user_id_by_username,
    get_next_ar_id_from_db,
)
from alchemy.db.model import (
    AnnotationGuide,
    AnnotationRequest,
    AnnotationRequestStatus,
    AnnotationValue,
    ClassificationAnnotation,
    Task,
    User,
    db,
    fetch_annotation_entity_and_ids_done_by_user_under_labels,
    get_or_create,
)
from .auth import login_required

bp = Blueprint("tasks", __name__, url_prefix="/tasks")


@bp.route("/<string:id>")
@login_required
def show(id):
    username = g.user["username"]

    import time

    st = time.time()

    task = db.session.query(Task).filter(Task.id == id).first()
    ar_id_and_status_pairs = fetch_ar_id_and_status(
        dbsession=db.session, task_id=id, username=username
    )

    annotation_entity_and_ids_done_by_user_for_task = fetch_annotation_entity_and_ids_done_by_user_under_labels(
        dbsession=db.session, username=username, labels=task.get_labels()
    )

    et = time.time()
    print("Load time", et - st)

    return render_template(
        "tasks/show.html",
        task=task,
        annotated=annotation_entity_and_ids_done_by_user_for_task,
        ars=[item[0] for item in ar_id_and_status_pairs],
        has_annotation=[
            item[1] == AnnotationRequestStatus.Complete
            for item in ar_id_and_status_pairs
        ],
    )


@bp.route("/<string:task_id>/examine/<string:user_under_exam>")
@login_required
def examine(task_id, user_under_exam):
    # TODO we should add UserRole control. Right now I'm just assuming only
    #  the admin can see the admin server control page.
    task = db.session.query(Task).filter(Task.id == task_id).first()
    annotation_entity_and_ids_done_by_user_for_task = fetch_annotation_entity_and_ids_done_by_user_under_labels(
        dbsession=db.session, username=user_under_exam, labels=task.get_labels()
    )

    return render_template(
        "tasks/examine.html",
        task=task,
        annotated=annotation_entity_and_ids_done_by_user_for_task,
        user_under_exam=user_under_exam,
    )


@bp.route("/<string:task_id>/compare")
@login_required
def compare_annotations(task_id):
    params = url_decode(request.query_string)
    user1 = params.get("user1", None)
    user2 = params.get("user2", None)
    label = params.get("label", None)

    if user1 and user2:
        logging.info("Comparing annotations for {} and {}".format(user1, user2))
        users_to_compare = [user1, user2]
    else:
        logging.info(
            "No complete user pair provided. Show annotations for " "all users."
        )
        task = get_or_create(dbsession=db.session, model=Task, id=task_id)
        users_to_compare = task.get_annotators()

    comparison_df, id_df = _construct_comparison_df(
        dbsession=db.session, label=label, users_to_compare=users_to_compare
    )

    return render_template(
        "tasks/compare.html",
        task_id=task_id,
        users=list(comparison_df.columns),
        entities=list(comparison_df.index.values),
        label=label,
        comparison_df=comparison_df,
        id_df=id_df,
    )


@bp.route("/<string:task_id>/annotate/<string:ar_id>")
@login_required
def annotate(task_id, ar_id):
    annotator = g.user["username"]
    task, anno, next_example_id = _prepare_annotation_common(
        task_id=task_id, example_id=ar_id, is_request=True, username=annotator
    )

    anno["update_redirect_link"] = url_for("tasks.show", id=task_id)
    anno["task_page_name"] = "Task"

    anno["total_size"] = count_ar_under_task_and_user(
        dbsession=db.session, task_id=task_id, username=annotator
    )

    anno["item_id"] = count_completed_ar_under_task_and_user(
        dbsession=db.session, task_id=task_id, username=annotator
    )

    return render_template(
        "tasks/annotate.html",
        task=task,
        anno=anno,
        data=json.dumps([anno]),
        next_ar_id=next_example_id,
    )


@bp.route("/receive_annotation", methods=["POST"])
@login_required
def receive_annotation():
    """API meant for Javascript to consume"""
    username = g.user["username"]
    user_id = fetch_user_id_by_username(db.session, username=username)

    data = json.loads(request.data)
    task_id = data["task_id"]
    ar_id = data["req"]["ar_id"]
    entity_type = data["req"]["entity_type"]
    entity = data["req"]["entity"]

    context = {"data": data["req"]["data"], "pattern_info": data["req"]["pattern_info"]}

    annotation_result = data["anno"]["labels"]
    annotation_logging_msg = ""
    request_logging_msg = ""
    for label in annotation_result:
        value = annotation_result[label]

        annotation = (
            db.session.query(ClassificationAnnotation)
            .filter(
                ClassificationAnnotation.entity_type == entity_type,
                ClassificationAnnotation.entity == entity,
                ClassificationAnnotation.label == label,
                ClassificationAnnotation.user_id == user_id,
            )
            .one_or_none()
        )

        if annotation:
            annotation.value = value
            annotation_logging_msg = (
                f"Updated existing annotation for "
                f"{entity} under label {label} with "
                f"new value {value}"
            )
            request_logging_msg = ""
        else:
            annotation = ClassificationAnnotation(
                entity_type=entity_type,
                entity=entity,
                label=label,
                user_id=user_id,
                context=context,
                value=value,
            )
            annotation_logging_msg = (
                f"Created an instance of " f"annotation {annotation}"
            )

            # TODO only mark request as complete if the incoming label matches
            #  the request label.
            annotatation_request = get_or_create(
                dbsession=db.session, model=AnnotationRequest, id=ar_id
            )
            annotatation_request.status = AnnotationRequestStatus.Complete
            db.session.add(annotatation_request)

            request_logging_msg = (
                f"Marked annotation request for {entity} " f"under {label} as complete."
            )

        db.session.add(annotation)

    try:
        db.session.commit()
        logging.info(annotation_logging_msg)
        logging.info(request_logging_msg)
    except DatabaseError as e:
        logging.error(e)
        db.session.rollback()
        raise e

    next_ar_id = get_next_ar_id_from_db(
        dbsession=db.session, task_id=task_id, user_id=user_id, current_ar_id=ar_id
    )

    if next_ar_id:
        return {
            "redirect": url_for("tasks.annotate", task_id=task_id, ar_id=next_ar_id)
        }
    else:
        return {"redirect": url_for("tasks.show", id=task_id)}


@bp.route("/<string:task_id>/reannotate/<string:annotation_id>")
@login_required
def reannotate(task_id, annotation_id):
    annotation_owner = request.args.get(
        "username", default=g.user["username"], type=str
    )
    task, anno, next_example_id = _prepare_annotation_common(
        task_id=task_id,
        example_id=annotation_id,
        is_request=False,
        username=annotation_owner,
    )

    anno["task_page_name"] = "Task"
    if request.referrer:
        anno["update_redirect_link"] = request.referrer
        if "examine" in request.referrer:
            anno["task_page_name"] = "Examine"
        elif "compare" in request.referrer:
            anno["task_page_name"] = "Compare"
    else:
        anno["update_redirect_link"] = "/"

    return render_template(
        "tasks/annotate.html",
        task=task,
        anno=anno,
        data=json.dumps([anno]),
        next_annotation_id=next_example_id,
    )


@bp.route("/update_annotation", methods=["POST"])
@login_required
def update_annotation():
    """API meant for Javascript to consume"""
    # TODO need to check if this is for admin correction flow.
    username = g.user["username"]

    data = json.loads(request.data)
    annotation_owner = get_or_create(
        dbsession=db.session, model=User, username=data["username"]
    )

    entity_type = data["req"]["entity_type"]
    entity = data["req"]["entity"]

    annotation_result = data["anno"]["labels"]

    context = {"data": data["req"]["data"], "pattern_info": data["req"]["pattern_info"]}

    for label in annotation_result:
        value = annotation_result[label]
        annotation = (
            db.session.query(ClassificationAnnotation)
            .filter(
                ClassificationAnnotation.label == label,
                ClassificationAnnotation.entity == entity,
                ClassificationAnnotation.entity_type == entity_type,
                ClassificationAnnotation.user_id == annotation_owner.id,
            )
            .one_or_none()
        )
        if annotation:
            annotation.value = value
            annotation.context = context
        else:
            # In case the user forgot to annotate this label in the last pass.
            annotation = ClassificationAnnotation(
                label=label,
                entity_type=entity_type,
                entity=entity,
                user_id=annotation_owner.id,
                value=value,
                context=context,
            )
        db.session.add(annotation)
    try:
        db.session.commit()
    except DatabaseError:
        db.session.rollback()
        raise

    return {"redirect": data.get("update_redirect_link")}


def _prepare_annotation_common(
    task_id: int, example_id: int, username: str, is_request: bool = True
) -> Tuple[Task, Dict, int]:
    user_id = fetch_user_id_by_username(db.session, username=username)
    task = db.session.query(Task).filter(Task.id == task_id).first()

    if is_request:
        example_dict = construct_ar_request_dict(db.session, example_id)
        next_example_id = get_next_ar_id_from_db(
            dbsession=db.session,
            task_id=task_id,
            user_id=user_id,
            current_ar_id=example_dict["ar_id"],
        )
    else:
        example_dict = construct_annotation_dict(db.session, example_id)
        next_example_id = None

    annotations_on_entity_done_by_user = (
        db.session.query(ClassificationAnnotation)
        .filter(
            ClassificationAnnotation.entity == example_dict["entity"],
            ClassificationAnnotation.user_id == user_id,
        )
        .all()
    )

    # NOTE: hack!
    # backfill the domain and name, in case they'remissing
    meta_dict = {"name": example_dict["entity"], "domain": example_dict["entity"]}
    if example_dict["data"].get("meta") is None:
        example_dict["data"]["meta"] = meta_dict

    anno = build_empty_annotation(example_dict)
    for existing_annotation in annotations_on_entity_done_by_user:
        if existing_annotation.label in task.get_labels():
            anno["anno"]["labels"][
                existing_annotation.label
            ] = existing_annotation.value

    anno["task_id"] = task.id
    anno["annotation_guides"] = {}
    anno["suggested_labels"] = task.get_labels()
    anno["username"] = username

    # Make sure the requested label is in the list.
    if example_dict["label"] not in anno["suggested_labels"]:
        anno["suggested_labels"].insert(0, example_dict["label"])

    for label in anno["suggested_labels"]:
        if label not in anno["anno"]["labels"]:
            anno["anno"]["labels"][label] = AnnotationValue.NOT_ANNOTATED

        # TODO optimize query
        guide = db.session.query(AnnotationGuide).filter_by(label=label).first()
        if guide:
            anno["annotation_guides"][label] = {"html": guide.get_html()}

    anno["is_new_annotation"] = bool(is_request)

    return task, anno, next_example_id
