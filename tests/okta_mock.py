from functools import wraps

from flask import redirect


class _Auth:
    def __init__(self):
        self.bypass = True

    def update_login_decorator(self, decorator):
        pass

    def login_required(self, f):
        @wraps(f)
        def _wrapper(*args, **kwargs):
            if self.bypass:
                return f(*args, **kwargs)

            return redirect('https://dev-8588821.okta.com/oauth2/default/v1/authorize')
        return _wrapper
