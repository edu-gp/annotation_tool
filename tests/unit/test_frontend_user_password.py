from shared.frontend_user_password import get_frontend_user_password, generate_frontend_user_login_link


def test_get_frontend_user_password(monkeypatch):
    monkeypatch.setenv('ANNOTATION_TOOL_FRONTEND_SECRET', '1234')

    assert get_frontend_user_password('ed') == \
        'b44fda2b617f2c5607f654eaefc17b7e50056234449dc51bf2533d6ad9d3338e'


def test_generate_frontend_user_login_link(monkeypatch):
    monkeypatch.setenv('ANNOTATION_TOOL_FRONTEND_SECRET', '1234')
    monkeypatch.setenv('ANNOTATION_TOOL_FRONTEND_SERVER',
                       'http://localhost:5001')

    assert generate_frontend_user_login_link('ed') == \
        'http://localhost:5001/auth/login?username=ed&password=b44fda2b617f2c5607f654eaefc17b7e50056234449dc51bf2533d6ad9d3338e'
