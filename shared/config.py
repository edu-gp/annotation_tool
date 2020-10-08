import os
from os import environ


class Config:
    """
    Set configuration vars from .env file.

    This module is designed to fail loudly when a required env var is not found.
    """
    @staticmethod
    def get_admin_server_password():
        return environ['ANNOTATION_TOOL_ADMIN_SERVER_PASSWORD']

    @staticmethod
    def get_annotation_server_secret():
        return environ['ANNOTATION_TOOL_ANNOTATION_SERVER_SECRET']

    @staticmethod
    def get_annotation_server():
        '''e.g. http://localhost:5001'''
        return environ['ANNOTATION_TOOL_ANNOTATION_SERVER_SERVER']

    @staticmethod
    def get_tasks_dir():
        d = environ.get('ANNOTATION_TOOL_TASKS_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__tasks')
        return d

    @staticmethod
    def get_data_dir():
        d = environ.get('ANNOTATION_TOOL_DATA_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__data')
        return d

    @staticmethod
    def get_inference_cache_dir():
        d = environ.get('ANNOTATION_TOOL_INFERENCE_CACHE_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__infcache')
        return d
