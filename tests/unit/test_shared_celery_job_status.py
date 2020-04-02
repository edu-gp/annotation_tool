from shared.celery_job_status import CeleryJobStatus


def test_celery_job_status():
    celery_id = 'test_12345'
    context_id = 'myapp:blah'
    CeleryJobStatus(celery_id, context_id).save()

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert cjs.state == 'QUEUED'
    assert cjs.progress == 0.0

    cjs.set_state_started()

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert cjs.state == 'STARTED'
    assert cjs.progress == 0.0

    cjs.set_progress(0.5)

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert cjs.state == 'STARTED'
    assert cjs.progress == 0.5

    cjs.set_state_done()
    cjs.set_progress(1.0)

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    assert cjs.state == 'DONE'
    assert cjs.progress == 1.0

    cjs_list = CeleryJobStatus.fetch_all_by_context_id(context_id)
    assert len(cjs_list) == 1
    assert cjs_list[0].state == 'DONE'
    assert cjs_list[0].progress == 1.0

    cjs.delete()
    assert CeleryJobStatus.fetch_by_celery_id(celery_id) is None
