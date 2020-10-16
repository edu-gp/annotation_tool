from alchemy.shared import celery_job_status
from alchemy.shared.celery_job_status import (
    create_status, delete_status, set_status,
    CeleryJobStatus, JobStatus
)


class FakeRedis:
    def __init__(self):
        self.store = {}

    def set(self, k, v):
        self.store[k] = str(v).encode()

    def get(self, k):
        return self.store.get(k)

    def delete(self, k):
        del self.store[k]

    def sadd(self, k, v):
        self.store[k] = self.store.get(k, set())
        self.store[k].add(v)

    def srem(self, k, v):
        if k in self.store and isinstance(self.store[k], set):
            self.store[k].remove(v)


def test_celery_job_status(monkeypatch):
    fake_redis = FakeRedis()

    monkeypatch.setattr(celery_job_status, 'get_redis', lambda: fake_redis)

    # Job is kicked off from the application, a celery_id is received
    # asynchronously from the celery worker.
    celery_id = 'test_12345'
    context_id = 'myapp:blah'

    # Application creates a status.
    create_status(celery_id, context_id)

    assert len(fake_redis.store) == 3

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert cjs.state == JobStatus.INIT
    assert cjs.progress == 0.0

    # At almost the same time, Celery picks up the job and works on it.

    set_status(celery_id, JobStatus.STARTED, progress=0.0)

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert str(cjs) == 'STARTED - 0.00% complete'

    set_status(celery_id, JobStatus.STARTED, progress=0.5)

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert str(cjs) == 'STARTED - 50.00% complete'

    set_status(celery_id, JobStatus.STARTED, progress=0.99)

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert str(cjs) == 'STARTED - 99.00% complete'

    set_status(celery_id, JobStatus.DONE)

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert str(cjs) == 'DONE'

    # Finally, the application deletes the job status.
    delete_status(celery_id, context_id)

    assert CeleryJobStatus.fetch_by_celery_id(celery_id) is None
