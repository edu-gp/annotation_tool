import logging

from envparse import env


def is_enabled():
    if env('PYTEST_CURRENT_TEST', default=None) is not None:
        return False

    return env.bool('USE_CLOUD_LOGGING', default=False)


def setup(name):
    from google.cloud import logging as glog
    from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging

    client = glog.Client()
    handler = CloudLoggingHandler(client, name=name)
    logging.getLogger().setLevel(logging.INFO)
    setup_logging(handler)
