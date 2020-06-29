from flask import (
    Blueprint, flash, redirect, render_template, request
)
from db.model import (
    db, EntityTypeEnum,
    ClassificationAnnotation, User, get_or_create
)
from .annotations_utils import parse_form

from .auth import auth

bp = Blueprint('annotations', __name__, url_prefix='/annotations')


@auth.login_required
def _before_request():
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route('/', methods=['GET'])
def index():
    return render_template('annotations/index.html')


@bp.route('/bulk', methods=['GET'])
def bulk():
    # TODO these two fields are not added to the html form
    request.form = {
        'user': request.args.get('user'),
        'label': request.args.get('label'),
        'entity_type': request.args.get('entity_type')
    }

    return render_template('annotations/bulk.html',
                           redirect_to=request.referrer,
                           entity_types=EntityTypeEnum.get_all_entity_types())


@bp.route('/bulk', methods=['POST'])
def bulk_post():
    import logging
    logging.error(request.form)
    logging.error(request.form.get('entities'))

    try:
        # Validate Form

        redirect_to = request.form['redirect_to'] or '/'

        user, label, entities, annotations, entity_type = \
            parse_form(request.form)

        # TODO should we add check on the User and Label?
        #  We should only accept upload from existing users and labels.

        # Insert into Database

        user = get_or_create(db.session, User, username=user)

        # TODO: Best way to do upsert is using db-specific functionality,
        # e.g. https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
        # But that might breaks our support for SQLITE.
        # For now, here's a less efficient solution.
        # Note: We can't use `get_or_create` since 'value' is a required field.

        for entity, annotation in zip(entities, annotations):
            anno = db.session.query(ClassificationAnnotation).filter_by(
                entity_type=entity_type, entity=entity,
                user=user, label=label
            ).first()

            if anno is None:
                if entity_type == EntityTypeEnum.COMPANY:
                    context = {
                        "text": "N/A",
                        "meta": {
                            "name": entity,
                            "domain": entity
                        }
                    }
                else:
                    context = {
                        "text": "N/A",
                        "meta": {
                            "name": entity
                            # TODO we probably should name `domain` to
                            #  something else according to the entity type
                        }
                    }
                anno = ClassificationAnnotation(
                    entity_type=entity_type, entity=entity,
                    user=user, label=label,
                    value=annotation,
                    context=context
                )
            else:
                anno.value = annotation

            db.session.add(anno)

        try:
            db.session.commit()
        except Exception:
            db.session.rollback()
            raise

        flash(f"Inserted or updated {len(annotations)} annotations for "
              f'label="{label}" from user="{user.username}"')

    except Exception as e:
        flash(str(e))
        return render_template('annotations/bulk.html')
    else:
        return redirect(redirect_to)
