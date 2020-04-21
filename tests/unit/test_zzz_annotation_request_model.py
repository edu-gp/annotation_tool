from db.model import (
    Database, AnnotationRequest, BackgroundJob, JobType, JobStatus,
    User, Context, Task, AnnotationType
)
from db.config import TestingConfig


class TestAnnotationRequestModel:
    def setup_method(self, test_method):
        db = Database(TestingConfig.SQLALCHEMY_DATABASE_URI)

        db.create_all()

        job = BackgroundJob(
            type=JobType.AnnotationRequestGenerator,
            params={}, output={}, status=JobStatus.INIT)
        db.session.add(job)

        user = User(username="foo")
        db.session.add(user)

        db.session.commit()

        self.db = db

    def teardown_method(self, test_method):
        self.db.session.remove()
        self.db.drop_all()

    def test_sanity(self):
        assert len(User.query.all()) == 1

    def test_create_annotation_request(self):
        job = BackgroundJob.query.first()
        user = User.query.first()

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
        self.db.session.add(req)
        self.db.session.commit()

        assert len(AnnotationRequest.query.all()) == 1
        assert len(Task.query.all()) == 1
        assert len(Context.query.all()) == 1

    def test_create_minimal_annotation_request(self):
        """
        AnnotationRequest can be created without a task and a source.
        """
        user = User.query.first()

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
        self.db.session.add(req)
        self.db.session.commit()

        assert len(AnnotationRequest.query.all()) == 1
        assert len(Task.query.all()) == 0
        assert len(Context.query.all()) == 1

        assert req.task is None
