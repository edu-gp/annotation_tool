import hashlib
import logging
import time


def get_ar_id(fname, line_number):
    ar_id = f"{fname}:{line_number}"
    ar_id = hashlib.sha224(ar_id.encode()).hexdigest()
    return ar_id


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if "log_time" in kw:
            name = kw.get("log_name", method.__name__.upper())
            kw["log_time"][name] = int((te - ts) * 1000)
        else:
            logging.info("%r %2.2f ms" % (method.__name__, (te - ts) * 1000))
        return result

    return timed
