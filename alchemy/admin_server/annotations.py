import logging

from flask import Blueprint, flash, redirect, render_template, request

from alchemy.data.request.annotation_request import AnnotationUpsertRequest
from alchemy.db.model import (
    AnnotationValue,
    ClassificationAnnotation,
    EntityTypeEnum,
    User,
    db,
)
from alchemy.shared.component import annotation_dao
from .annotations_utils import parse_bulk_upload_v2_form, parse_form
from .auth import auth

bp = Blueprint("annotations", __name__, url_prefix="/annotations")


@auth.login_required
def _before_request():
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route("/", methods=["GET"])
def index():
    return render_template("annotations/index.html")


@bp.route("/bulk_upload_positive_annotations", methods=["GET"])
def bulk_upload_positive_annotations():
    # TODO The user should be the current user but we don't have that in
    #  the session yet.
    request.form = {"user": request.args.get("user")}

    acceptable_values = [
        AnnotationValue.POSITIVE,
        AnnotationValue.NEGTIVE,
        AnnotationValue.UNSURE,
    ]

    return render_template(
        "annotations/bulk_upload_positive_annotations.html",
        annotation_values=acceptable_values,
        redirect_to=request.referrer,
        entity_types=EntityTypeEnum.get_all_entity_types(),
    )


@bp.route("/bulk_upload_positive_annotations", methods=["POST"])
def bulk_post_positive_annotations():
    try:
        # Validate Form
        logging.error(request.form)

        redirect_to = request.form["redirect_to"] or "/"

        user, entities, labels, entity_type, value = parse_bulk_upload_v2_form(
            request.form
        )

        # Insert into Database
        user = (db.session.query(User)
                .filter(username=user)
                .one_or_none())
        if not user:
            raise ValueError(f"Annotator {user} is not registered on the website.")

        for entity, label in zip(entities, labels):
            anno = _upsert_annotations(
                dbsession=db.session,
                entity_type=entity_type,
                entity=entity,
                label=label,
                user_id=user.id,
                value=value,
            )

            db.session.add(anno)

        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise

        flash(
            f"Inserted or updated {len(entities)} annotations "
            f'from user="{user.username}"'
        )

    except Exception as e:
        logging.error(e)
        flash(str(e))
        return render_template("annotations/bulk_upload_positive_annotations.html")
    else:
        return redirect(redirect_to)


@bp.route("/bulk", methods=["GET"])
def bulk():
    # TODO these two fields are not added to the html form
    request.form = {
        "user": request.args.get("user"),
        "label": request.args.get("label"),
        "entity_type": request.args.get("entity_type"),
    }

    return render_template(
        "annotations/bulk.html",
        redirect_to=request.referrer,
        entity_types=EntityTypeEnum.get_all_entity_types(),
    )


@bp.route("/bulk", methods=["POST"])
def bulk_post():
    try:
        # Validate Form
        redirect_to = request.form["redirect_to"] or "/"

        user, label, entities, values, entity_type, is_golden = parse_form(request.form)

        # TODO should we add check on the User and Label?
        #  We should only accept upload from existing users and labels.

        # Insert into Database

        user = (db.session.query(User)
                .filter(username=user)
                .one_or_none())
        if not user:
            raise ValueError(f"Annotator {user} is not registered on the website.")

        # TODO: Best way to do upsert is using db-specific functionality,
        # e.g. https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
        # But that might breaks our support for SQLITE.
        # For now, here's a less efficient solution.
        # Note: We can't use `get_or_create` since 'value' is a required field.

        requests = []
        for entity, value in zip(entities, values):
            # TODO: Once we have SOA, we won't have to create a dict like this.
            dict_data = {
                "entity_type": entity_type,
                "entity": entity,
                "label": label,
                "user_id": user.id,
                "value": value,
                "context": __construct_context(entity_type, entity),
            }
            requests.append(AnnotationUpsertRequest.from_dict(dict_data=dict_data))

        annotation_dao.upsert_annotations_bulk(requests)

        flash(
            f"Inserted or updated {len(values)} annotations for "
            f'label="{label}" from user="{user.username}"'
        )

    except Exception as e:
        logging.error(e)
        flash(str(e))
        return render_template("annotations/bulk.html")
    else:
        return redirect(redirect_to)


def _upsert_annotations(dbsession, entity_type, entity, label, user_id, value):
    annotation = (
        dbsession.query(ClassificationAnnotation)
        .filter_by(entity_type=entity_type, entity=entity, user_id=user_id, label=label)
        .one_or_none()
    )

    if annotation is None:
        context = __construct_context(entity_type, entity)
        annotation = ClassificationAnnotation(
            entity_type=entity_type,
            entity=entity,
            user_id=user_id,
            label=label,
            value=value,
            context=context,
        )
    else:
        annotation.value = value
    return annotation


def __construct_context(entity_type, entity):
    if entity_type == EntityTypeEnum.COMPANY:
        context = {"text": "N/A", "meta": {"name": entity, "domain": entity}}
    else:
        context = {
            "text": "N/A",
            "meta": {
                "name": entity
                # TODO we probably should name `domain` to
                #  something else according to the entity type
            },
        }
    return context
