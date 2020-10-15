import os
from envparse import env


class Config:
    """
    Set configuration vars from .env file.

    This module is designed to fail loudly when a required env var is not found.
    """
    @staticmethod
    def get_admin_server_password():
        return env('ANNOTATION_TOOL_ADMIN_SERVER_PASSWORD')

    @staticmethod
    def get_annotation_server_secret():
        return env('ANNOTATION_TOOL_ANNOTATION_SERVER_SECRET')

    @staticmethod
    def get_annotation_server():
        '''e.g. http://localhost:5001'''
        return env('ANNOTATION_TOOL_ANNOTATION_SERVER_SERVER')

    @staticmethod
    def get_tasks_dir():
        return env('ANNOTATION_TOOL_TASKS_DIR', default=os.path.join(os.getcwd(), '__tasks'))

    @staticmethod
    def get_data_dir():
        return env('ANNOTATION_TOOL_DATA_DIR', default=os.path.join(os.getcwd(), '__data'))

    @staticmethod
    def get_inference_cache_dir():
        return env('ANNOTATION_TOOL_INFERENCE_CACHE_DIR', default=os.path.join(os.getcwd(), '__infcache'))
