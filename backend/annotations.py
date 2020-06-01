from typing import List
from flask import (
    Blueprint, flash, redirect, render_template, request
)
from db.model import (
    db,
    EntityTypeEnum, AnnotationValue,
    ClassificationAnnotation, User, get_or_create
)

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

# TODO eddie: Hook this up to the Admin UI


@bp.route('/bulk', methods=['GET'])
def bulk():
    # TODO eddie: dev
    request.form = {
        'user': 'eddie',
        'label': 'HC',
        'domains': 'a.com\nb.com\nc.com',
        'annotations': '-1\n0\n1',
    }
    return render_template('annotations/bulk.html',
                           redirect_to=request.referrer)


@bp.route('/bulk', methods=['POST'])
def bulk_post():
    import logging
    logging.error(request.form)
    logging.error(request.form.get('domains'))

    try:
        # Validate Form

        redirect_to = request.form['redirect_to']

        user = request.form['user']
        label = request.form['label']
        domains = parse_list(request.form, 'domains')
        annotations = parse_list(request.form, 'annotations')

        assert user, 'User is required'
        assert label, 'Label is required'

        assert len(domains) == len(annotations), \
            f'Number of domains ({len(domains)}) does not match ' \
            f'with number of annotations ({len(annotations)})'

        # For now, we leave it up to the user to make sure no duplicates.
        assert len(set(domains)) == len(domains), \
            "There are duplicates in domains"

        acceptable_annotations = set([
            AnnotationValue.POSITIVE,
            AnnotationValue.NEGTIVE,
            AnnotationValue.UNSURE,
        ])

        for i in range(len(annotations)):
            annotations[i] = int(annotations[i])
            assert annotations[i] in acceptable_annotations, \
                f"Annotation {annotations[i]} is not in the list of " \
                f"acceptable annotations {acceptable_annotations}"

        # Insert into Database

        user = get_or_create(db.session, User, username=user)

        # TODO: Best way to do upsert is using db-specific functionality,
        # e.g. https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#insert-on-conflict-upsert
        # But that might breaks our support for SQLITE.
        # For now, here's a less efficient solution.
        # Note: We can't use `get_or_create` since 'value' is a required field.

        for domain, annotation in zip(domains, annotations):
            anno = db.session.query(ClassificationAnnotation).filter_by(
                entity_type=EntityTypeEnum.COMPANY, entity=domain,
                user=user, label=label
            ).first()

            if anno is None:
                anno = ClassificationAnnotation(
                    entity_type=EntityTypeEnum.COMPANY, entity=domain,
                    user=user, label=label,
                    value=annotation,
                    context={
                        "text": "N/A",
                        "meta": {
                            "name": domain,
                            "domain": domain
                        }
                    }
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


# TODO eddie: write test
def parse_list(form: dict, key: str) -> List:
    values = form.get(key)
    result = []
    if values:
        result = values.splitlines()
        result = [x for x in result if x]
    return result
