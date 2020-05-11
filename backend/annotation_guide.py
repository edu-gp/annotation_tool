from flask import (
    Blueprint, flash, redirect, render_template, request, url_for
)

from db.model import db, AnnotationGuide, get_or_create

from .auth import auth

bp = Blueprint('annotation_guide', __name__, url_prefix='/annotation_guide')


@auth.login_required
def _before_request():
    print("Before Request")
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route('', methods=['GET'])
def show():
    label = request.args['label']

    url = ''
    guide = db.session.query(AnnotationGuide).filter_by(label=label).first()
    if guide:
        url = guide.url

    return render_template(
        'annotation_guide/edit.html',
        label=label,
        url=url,
        redirect_to=request.args.get('redirect_to')
    )


@bp.route('', methods=['POST'])
def update():
    label = request.form['label']
    url = request.form['url']

    guide = get_or_create(db.session, AnnotationGuide, label=label)
    guide.url = url
    db.session.add(guide)
    db.session.commit()

    redirect_url = request.form.get('redirect_to')
    if redirect_url:
        return redirect(redirect_url)
    else:
        return redirect('/')
