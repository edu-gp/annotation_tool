import os
from os import environ

class Config:
    """
    Set configuration vars from .env file.
    
    This module is designed to fail loudly when a required env var is not found.
    """

    def get_backend_password():
        return environ['ANNOTATION_TOOL_BACKEND_PASSWORD']

    def get_frontend_secret():
        return environ['ANNOTATION_TOOL_FRONTEND_SECRET']

    def get_frontend_server():
        '''e.g. http://localhost:5001'''
        return environ['ANNOTATION_TOOL_FRONTEND_SERVER']

    def get_tasks_dir():
        d = environ.get('ANNOTATION_TOOL_TASKS_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__tasks')
        return d

    def get_data_dir():
        d = environ.get('ANNOTATION_TOOL_DATA_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__data')
        return d

    def get_inference_cache_dir():
        d = environ.get('ANNOTATION_TOOL_INFERENCE_CACHE_DIR')
        if d is None:
            d = os.path.join(os.getcwd(), '__infcache')
        return d
