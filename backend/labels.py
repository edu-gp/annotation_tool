from flask import (
    Blueprint, flash, redirect, render_template, request, url_for
)

from db.model import db, AnnotationGuide, LabelPatterns, get_or_create
from shared.utils import list_to_textarea, textarea_to_list
from .auth import auth

bp = Blueprint('labels', __name__, url_prefix='/labels')


@auth.login_required
def _before_request():
    print("Before Request")
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route('', methods=['GET'])
def edit():
    label = request.args['label']

    annotation_guide_text = ''
    guide = db.session.query(AnnotationGuide).filter_by(label=label).first()
    if guide:
        annotation_guide_text = guide.get_text()

    patterns = ''
    label_patterns = db.session.query(
        LabelPatterns).filter_by(label=label).first()
    if label_patterns and label_patterns.get_positive_patterns():
        patterns = list_to_textarea(label_patterns.get_positive_patterns())

    return render_template(
        'labels/edit.html',
        label=label,
        annotation_guide_text=annotation_guide_text,
        patterns=patterns,
        redirect_to=request.args.get('redirect_to')
    )


@bp.route('', methods=['POST'])
def update():
    error = None
    try:
        form = request.form

        label = request.form['label']
        annotation_guide_text = request.form['annotation_guide_text']
        patterns = parse_patterns(form)
    except Exception as e:
        error = str(e)

    if error is not None:
        flash(error)
        return render_template('labels/edit.html')
    else:
        guide = get_or_create(db.session, AnnotationGuide, label=label)
        guide.set_text(annotation_guide_text)
        db.session.add(guide)

        label_patterns = get_or_create(
            db.session, LabelPatterns, label=label)
        label_patterns.set_positive_patterns(patterns)
        db.session.add(label_patterns)

        db.session.commit()

        redirect_url = request.form.get('redirect_to')
        if redirect_url:
            return redirect(redirect_url)
        else:
            return redirect('/')


def parse_patterns(form):
    patterns = form['patterns']

    try:
        patterns = textarea_to_list(patterns)
    except Exception as e:
        raise Exception(f'Unable to load Patterns: {e}')

    assert isinstance(patterns, list), 'Patterns must be a list'
    return patterns
