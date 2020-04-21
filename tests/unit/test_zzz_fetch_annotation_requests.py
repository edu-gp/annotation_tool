from db.model import (
    Database, AnnotationRequest, BackgroundJob, JobType, JobStatus,
    User, Context, Task, AnnotationType
)
from db.config import TestingConfig


class TestFetchAnnotationRequests:
    def setup_method(self, test_method):
        db = Database(TestingConfig.SQLALCHEMY_DATABASE_URI)

        db.create_all()

        user_1 = User(username="foo")
        user_2 = User(username="bar")
        db.session.add_all([user_1, user_2])

        task_1 = Task(name="task 1", default_params={})
        task_2 = Task(name="task 2", default_params={})
        task_3 = Task(name="task 3", default_params={})
        db.session.add_all([task_1, task_2, task_3])

        def _req(name, user, task, order=None):
            return AnnotationRequest(
                user=user,
                context=Context(hash=name, data={'foo': 'bar'}),
                task=task,
                annotation_type=AnnotationType.ClassificationAnnotation,
                order=order,
                name=name
            )

        reqs = [
            _req('1', user_1, task_1, order=1),

            _req('2', user_1, task_2, order=3),
            _req('3', user_1, task_2, order=2),
            _req('4', user_1, task_2, order=1),

            _req('5', user_2, task_1, order=1),
            _req('6', user_2, task_1, order=2),

            _req('7', user_2, task_2, order=1),

            _req('8', user_2, task_3, order=1),

            # We also include some requests that do not come from tasks.
            _req('x', user_2, None),
            _req('y', user_2, None),
            _req('z', user_2, None),
        ]
        db.session.add_all(reqs)

        db.session.commit()

        self.users = [user_1, user_2]
        self.db = db

    def teardown_method(self, test_method):
        self.db.session.remove()
        self.db.drop_all()

    def test_sanity(self):
        assert len(AnnotationRequest.query.all()) == 11
        assert len(Task.query.all()) == 3
        assert len(User.query.all()) == 2

    def test_fetch_ar_count_per_task(self):
        """When an annotator logs in, she should be able to see how many
        requests per task are pending.
        """
        user_1, user_2 = self.users

        assert user_1.fetch_ar_count_per_task() == [
            ('task 1', 1, 1),  # First task has 1 request.
            ('task 2', 2, 3),  # Second task has 3 requests.
            # Third task has none and so is not shown.
        ]

        assert user_2.fetch_ar_count_per_task() == [
            ('task 1', 1, 2),  # First task has 2 requests.
            ('task 2', 2, 1),  # Second task has 1 request.
            ('task 3', 3, 1),  # Third task has 1 request.
        ]

    def test_fetch_annotation_request(self):
        """When an annotator clicks on a task, she should be able to see all
        the requests associated with that task.
        """
        user_1 = self.users[0]
        task_id = 2

        res = user_1.fetch_ar_for_task(task_id)

        assert len(res) == 3
        assert [r.name for r in res] == ["4", "3", "2"], "Order is wrong"
