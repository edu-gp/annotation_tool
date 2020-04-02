# import os
# import json
# from db import _celery_job_status_storage

import redis
import time


def get_redis():
    return redis.Redis(host='localhost', port=6379, db=0)


class CeleryJobStatus:
    def __init__(self, celery_id, context_id):
        self.celery_id = celery_id
        self.context_id = context_id
        self.state = 'QUEUED'  # STARTED, DONE, FAILED
        self.progress = 0.  # 0 to 1 float.
        self.created_at = time.time()
        self.updated_at = time.time()

    def __str__(self):
        if self.state == 'STARTED':
            return f"{self.state} - {100*self.progress:.2f}% complete"
        else:
            return self.state

    def is_stale(self):
        return (self.state == 'DONE' and (time.time() - self.updated_at > 10)) or \
            (time.time() - self.created_at > 60*60*5)  # 5 hours

    # -------------------------------------------------------------------------

    def set_state_started(self):
        self.state = 'STARTED'
        self.updated_at = time.time()
        self.save()

    def set_state_done(self):
        self.state = 'DONE'
        self.updated_at = time.time()
        self.save()

    def set_state_failed(self):
        self.state = 'FAILED'
        self.updated_at = time.time()
        self.save()

    def set_progress(self, progress):
        if progress >= 0. and progress <= 1.:
            self.progress = progress
            self.updated_at = time.time()
            self.save()
        # TODO else silently error.

    # -------------------------------------------------------------------------

    def save(self):
        r = get_redis()
        # TODO transaction - although very unlikely this'll be a problem
        r.set(f'cjs:{self.celery_id}:s', self.state)
        r.set(f'cjs:{self.celery_id}:p', self.progress)
        r.set(f'cjs:{self.celery_id}:c', self.created_at)
        r.set(f'cjs:{self.celery_id}:u', self.updated_at)
        r.set(f'cjs:{self.celery_id}:x', self.context_id)
        r.sadd(f'cjss:{self.context_id}', self.celery_id)

    def delete(self):
        r = get_redis()
        # TODO transaction - although very unlikely this'll be a problem
        r.delete(f'cjs:{self.celery_id}:s')
        r.delete(f'cjs:{self.celery_id}:p')
        r.delete(f'cjs:{self.celery_id}:c')
        r.delete(f'cjs:{self.celery_id}:u')
        r.delete(f'cjs:{self.celery_id}:x')
        r.srem(f'cjss:{self.context_id}', self.celery_id)

    @staticmethod
    def fetch_all_by_context_id(context_id):
        r = get_redis()
        res = []
        for celery_id in r.smembers(f'cjss:{context_id}'):
            celery_id = celery_id.decode()
            res.append(CeleryJobStatus.fetch_by_celery_id(celery_id))
        res = sorted(res, key=lambda cjs: cjs.created_at)
        return res

    @staticmethod
    def fetch_by_celery_id(celery_id):
        r = get_redis()

        _s = r.get(f'cjs:{celery_id}:s')
        _p = r.get(f'cjs:{celery_id}:p')
        _c = r.get(f'cjs:{celery_id}:c')
        _u = r.get(f'cjs:{celery_id}:u')
        _x = r.get(f'cjs:{celery_id}:x')

        if _s is None or _p is None or _c is None or _u is None or _x is None:
            return None
        else:
            cjs = CeleryJobStatus(None, None)
            cjs.state = _s.decode()
            cjs.progress = float(_p)
            cjs.created_at = float(_c)
            cjs.updated_at = float(_u)
            cjs.context_id = _x.decode()
            cjs.celery_id = celery_id
            return cjs
