import os
from os import environ


class Config:
    """
    Set configuration vars from .env file.

    This module is designed to fail loudly when a required env var is not found.
    """
    @classmethod
    def get_backend_password(cls):
        return environ['ANNOTATION_TOOL_BACKEND_PASSWORD']

    @classmethod
    def get_frontend_secret(cls):
        return environ['ANNOTATION_TOOL_FRONTEND_SECRET']

    @classmethod
    def get_frontend_server(cls):
        '''e.g. http://localhost:5001'''
        return environ['ANNOTATION_TOOL_FRONTEND_SERVER']

    @classmethod
    def get_tasks_dir(cls):
        d = environ.get('ANNOTATION_TOOL_TASKS_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__tasks')
        return d

    @classmethod
    def get_data_dir(cls):
        d = environ.get('ANNOTATION_TOOL_DATA_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__data')
        return d

    @classmethod
    def get_inference_cache_dir(cls):
        d = environ.get('ANNOTATION_TOOL_INFERENCE_CACHE_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__infcache')
        return d
