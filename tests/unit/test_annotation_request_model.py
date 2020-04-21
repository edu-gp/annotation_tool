from tests.sqlalchemy_conftest import *
from db.model import (
    AnnotationRequest, BackgroundJob, JobType, JobStatus,
    User, Context, Task, AnnotationType
)


def _populate_db(dbsession):
    job = BackgroundJob(
        type=JobType.AnnotationRequestGenerator,
        params={}, output={}, status=JobStatus.INIT)
    dbsession.add(job)

    user = User(username="foo")
    dbsession.add(user)

    dbsession.commit()


def test_sanity(dbsession):
    _populate_db(dbsession)
    assert len(dbsession.query(User).all()) == 1


def test_create_annotation_request(dbsession):
    _populate_db(dbsession)
    job = dbsession.query(BackgroundJob).first()
    user = dbsession.query(User).first()

    context = Context(
        hash='1234',
        data={'foo': 'bar'}
    )

    task = Task(name='My Task', default_params={})

    req = AnnotationRequest(
        user=user,
        context=context,
        task=task,
        annotation_type=AnnotationType.ClassificationAnnotation,
        order=12,
        name="Testing",
        additional_info={'domain': 'www.google.com'},
        source={'type': 'BackgroundJob', 'id': job.id}
    )
    dbsession.add(req)
    dbsession.commit()

    assert len(dbsession.query(AnnotationRequest).all()) == 1
    assert len(dbsession.query(Task).all()) == 1
    assert len(dbsession.query(Context).all()) == 1


def test_create_minimal_annotation_request(dbsession):
    _populate_db(dbsession)
    user = dbsession.query(User).first()

    context = Context(
        hash='1234',
        data={'foo': 'bar'}
    )

    req = AnnotationRequest(
        user=user,
        context=context,
        annotation_type=AnnotationType.ClassificationAnnotation,
        order=12,
        name="Testing"
    )
    dbsession.add(req)
    dbsession.commit()

    assert len(dbsession.query(AnnotationRequest).all()) == 1
    assert len(dbsession.query(Task).all()) == 0
    assert len(dbsession.query(Context).all()) == 1

    assert req.task is None
