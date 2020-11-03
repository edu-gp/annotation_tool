import importlib

from envparse import env

app_name = env('FLASK_APP')
app_module = importlib.import_module(app_name.replace('/', '.'))

if 'create_app' in dir(app_module):
    app = app_module.create_app()
elif 'make_app' in dir(app_module):
    app = app_module.make_app()
else:
    raise RuntimeError("Could not find the application factory in the flask app module.")

__all__ = [app]
