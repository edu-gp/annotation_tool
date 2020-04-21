from tests.sqlalchemy_conftest import *
from db.model import Task, BackgroundJob, JobType, JobStatus


def _populate_db(dbsession):
    task = Task(name='My Task', default_params={})
    dbsession.add(task)

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
    # db.session.bulk_save_objects([job_a, job_b, job_c])

    dbsession.commit()


def test_sanity(dbsession):
    _populate_db(dbsession)
    assert len(dbsession.query(Task).all()) == 1


def test_task_has_jobs(dbsession):
    _populate_db(dbsession)
    task = dbsession.query(Task).first()
    assert len(task.background_jobs) == 3


def test_job_has_task(dbsession):
    _populate_db(dbsession)
    assert len(dbsession.query(BackgroundJob).all()) == 3

    job = dbsession.query(BackgroundJob).first()
    assert job.task == dbsession.query(Task).first()


def test_create_a_new_job(dbsession):
    _populate_db(dbsession)
    task = dbsession.query(Task).first()
    job = BackgroundJob(type=99, params={}, output={}, status="blah")
    task.background_jobs.append(job)
    dbsession.commit()

    assert len(dbsession.query(Task).first().background_jobs) == 4


def test_create_a_job_without_a_task(dbsession):
    _populate_db(dbsession)
    job = BackgroundJob(type=99, params={}, output={}, status="blah")
    dbsession.add(job)
    dbsession.commit()
    assert len(dbsession.query(BackgroundJob).all()) == 4


def test_task_fetch_different_types_of_jobs(dbsession):
    _populate_db(dbsession)
    task = dbsession.query(Task).first()
    assert len(task.annotation_request_generator_jobs) == 2
    assert len(task.text_classification_model_training_jobs) == 1
