from flask import url_for

from shared.annotation_server_path_finder import get_annotation_server_user_password, \
    generate_annotation_server_user_login_link, generate_annotation_server_compare_link


def test_get_annotation_server_user_password(monkeypatch):
    monkeypatch.setenv('ANNOTATION_TOOL_ANNOTATION_SERVER_SECRET', '1234')

    assert get_annotation_server_user_password('ed') == \
        'b44fda2b617f2c5607f654eaefc17b7e50056234449dc51bf2533d6ad9d3338e'


def test_generate_annotation_server_user_login_link(monkeypatch):
    monkeypatch.setenv('ANNOTATION_TOOL_ANNOTATION_SERVER_SECRET', '1234')
    monkeypatch.setenv('ANNOTATION_TOOL_ANNOTATION_SERVER_SERVER',
                       'http://localhost:5001')

    assert generate_annotation_server_user_login_link('ed') == \
        'http://localhost:5001/auth/login?username=ed&password=b44fda2b617f2c5607f654eaefc17b7e50056234449dc51bf2533d6ad9d3338e'


def test_generate_annotation_server_compare_link(monkeypatch):
    monkeypatch.setenv('ANNOTATION_TOOL_ANNOTATION_SERVER_SERVER',
                       'http://localhost:5001')

    task_id = 1
    label1 = "hotdog"
    label2 = "hot dog"
    label3 = "hot/dog"
    users = {
        "user1": "aaa",
        "user2": "bbb"
    }

    url1 = generate_annotation_server_compare_link(
        task_id=task_id,
        label=label1,
        users_dict=users
    )
    assert url1 == "http://localhost:5001/tasks/1/compare?label=hotdog&user1=aaa&user2=bbb"

    url2 = generate_annotation_server_compare_link(
        task_id=task_id,
        label=label2,
        users_dict=users
    )
    assert url2 == "http://localhost:5001/tasks/1/compare?label=hot+dog" \
                   "&user1=aaa&user2=bbb"

    url3 = generate_annotation_server_compare_link(
        task_id=task_id,
        label=label1
    )
    assert url3 == "http://localhost:5001/tasks/1/compare?label=hotdog"

    url4 = generate_annotation_server_compare_link(
        task_id=task_id,
        label=label3
    )
    print(url4)