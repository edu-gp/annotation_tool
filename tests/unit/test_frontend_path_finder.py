from alchemy.shared.annotation_server_path_finder import (
    generate_annotation_server_compare_link,
)


def test_generate_annotation_server_compare_link(monkeypatch):
    monkeypatch.setenv(
        "ANNOTATION_TOOL_ANNOTATION_SERVER_SERVER", "http://localhost:5001"
    )

    task_id = 1
    label1 = "hotdog"
    label2 = "hot dog"
    label3 = "hot/dog"
    users = {"user1": "aaa", "user2": "bbb"}

    url1 = generate_annotation_server_compare_link(
        task_id=task_id, label=label1, users_dict=users
    )
    assert (
        url1 == "http://localhost:5001/tasks/1/compare?label=hotdog&user1=aaa&user2=bbb"
    )

    url2 = generate_annotation_server_compare_link(
        task_id=task_id, label=label2, users_dict=users
    )
    assert (
        url2 == "http://localhost:5001/tasks/1/compare?label=hot+dog"
        "&user1=aaa&user2=bbb"
    )

    url3 = generate_annotation_server_compare_link(task_id=task_id, label=label1)
    assert url3 == "http://localhost:5001/tasks/1/compare?label=hotdog"

    url4 = generate_annotation_server_compare_link(task_id=task_id, label=label3)
    print(url4)
