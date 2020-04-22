from tests.sqlalchemy_conftest import *
from db.model import DataSnapshotJob, JobStatus


def _populate_db(dbsession):
    invalid_job = DataSnapshotJob()
    dbsession.add(invalid_job)

    valid_job = DataSnapshotJob.create('positive', now=1)
    dbsession.add(valid_job)

    dbsession.commit()

    return [invalid_job, valid_job]


def test_sanity(dbsession):
    invalid_job, valid_job = _populate_db(dbsession)

    assert invalid_job.params == {}
    assert invalid_job.get_output_fname() is None
    assert valid_job.params != {}
    assert valid_job.get_output_fname() == '1_positive.jsonl'


def test_run(dbsession):
    invalid_job, valid_job = _populate_db(dbsession)

    def test_execute_fn(label_name, output_fname):
        pass

    invalid_job.run(execute_fn=test_execute_fn)
    assert invalid_job.status == JobStatus.Failed
    assert invalid_job.get_error() is not None

    valid_job.run(execute_fn=test_execute_fn)
    assert valid_job.status == JobStatus.Complete
    assert valid_job.get_error() is None


def test_bad_label_name(dbsession):
    valid_job = DataSnapshotJob.create(
        'this is a / dangerous . label /:=', now=123)
    dbsession.add(valid_job)
    dbsession.commit()

    assert dbsession.query(DataSnapshotJob).first().get_output_fname() == \
        '123_this_is_a_dangerous_._label.jsonl'
