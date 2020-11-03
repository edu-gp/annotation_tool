import importlib

from envparse import env

app_name = env('FLASK_APP')
app_module = importlib.import_module(app_name.replace('/', '.'))

app = app_module.create_app()
