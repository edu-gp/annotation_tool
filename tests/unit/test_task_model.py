from db.model import Database, Task, BackgroundJob, JobType, JobStatus
from db.config import TestingConfig


class TestTaskModel:
    def setup_method(self, test_method):
        db = Database(TestingConfig.SQLALCHEMY_DATABASE_URI)

        db.create_all()

        task = Task(name='My Task', default_params={})
        db.session.add(task)

        job_a = BackgroundJob(
            type=JobType.AnnotationRequestGenerator,
            params={}, output={}, status=JobStatus.INIT)
        job_b = BackgroundJob(
            type=JobType.AnnotationRequestGenerator,
            params={}, output={}, status=JobStatus.INIT)

        job_c = BackgroundJob(
            type=JobType.TextClassificationModelTraining,
            params={}, output={}, status=JobStatus.INIT)

        # Check it out, we can modify one-to-many like a python list!
        task.background_jobs.append(job_a)
        task.background_jobs += [job_b, job_c]

        # Note: Do NOT save these again; this will save 3 additional jobs,
        #       but each without a task_id.
        #db.session.bulk_save_objects([job_a, job_b, job_c])

        db.session.commit()

        self.db = db

    def teardown_method(self, test_method):
        self.db.session.remove()
        self.db.drop_all()

    def test_sanity(self):
        assert len(Task.query.all()) == 1

    def test_task_has_jobs(self):
        task = Task.query.first()
        assert len(task.background_jobs) == 3

    def test_job_has_task(self):
        assert len(BackgroundJob.query.all()) == 3

        job = BackgroundJob.query.first()
        assert job.task == Task.query.first()

    def test_create_a_new_job(self):
        task = Task.query.first()
        job = BackgroundJob(type=99, params={}, output={}, status="blah")
        task.background_jobs.append(job)
        self.db.session.commit()

        assert len(Task.query.first().background_jobs) == 4

    def test_create_a_job_without_a_task(self):
        job = BackgroundJob(type=99, params={}, output={}, status="blah")
        self.db.session.add(job)
        self.db.session.commit()
        assert len(BackgroundJob.query.all()) == 4

    def test_task_fetch_different_types_of_jobs(self):
        task = Task.query.first()
        assert len(task.annotation_request_generator_jobs) == 2
        assert len(task.text_classification_model_training_jobs) == 1
