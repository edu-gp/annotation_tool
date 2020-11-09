from mockito import when
from sqlalchemy.exc import DBAPIError

from alchemy.dao.task_dao import TaskDao
from alchemy.data.request.task_request import TaskCreateRequest
from alchemy.db.model import EntityTypeEnum, Task

entity_type = EntityTypeEnum.COMPANY
name = "b2c"
labels = ["hotdog"]
annotators = ["user1", "user2"]
data_files = ["file1.txt", "file2.txt"]


def _count_task(dbsession):
    return dbsession.query(Task).count()


def test_task_create_success(dbsession):
    task_dao = TaskDao(dbsession=dbsession)
    create_request = TaskCreateRequest(
        name=name,
        entity_type=entity_type,
        labels=labels,
        annotators=annotators,
        data_files=data_files,
    )

    task = task_dao.create_task(create_request=create_request)

    num_of_tasks = _count_task(dbsession)
    assert num_of_tasks == 1

    assert task is not None
    assert task.name == name
    assert set(task.get_labels()) == set(labels)
    assert set(task.get_data_filenames()) == set(data_files)
    assert set(task.get_annotators()) == set(annotators)


def test_task_create_with_invalid_request(dbsession):
    task_dao = TaskDao(dbsession=dbsession)
    create_request = TaskCreateRequest.from_dict({"name": name})

    try:
        _ = task_dao.create_task(create_request=create_request)
    except Exception as e:
        assert "Invalid request:" in str(e)

    num_of_tasks = _count_task(dbsession)
    assert num_of_tasks == 0


def test_task_create_with_duplicate_labels(dbsession):
    label = "healthecare"
    task1 = Task(name="task1")
    task1.set_labels([label])
    task1.set_annotators(annotators)
    task1.set_entity_type(entity_type)
    task1.set_data_filenames(data_files)
    dbsession.add(task1)
    dbsession.commit()

    task_dao = TaskDao(dbsession=dbsession)
    create_request = TaskCreateRequest(
        name=name,
        entity_type=entity_type,
        labels=[label, "b2b"],
        annotators=annotators,
        data_files=data_files,
    )

    try:
        _ = task_dao.create_task(create_request=create_request)
    except Exception as e:
        assert f"Label {label} is already created in task {task1.name}" in str(e)

    num_of_tasks = _count_task(dbsession)
    assert num_of_tasks == 1

    existing_task = dbsession.query(Task).one_or_none()

    assert existing_task.id == task1.id


def _prepare_for_retry_testcases(dbsession):
    task_dao = TaskDao(dbsession=dbsession)
    create_request = TaskCreateRequest(
        name=name,
        entity_type=entity_type,
        labels=labels,
        annotators=annotators,
        data_files=data_files,
    )
    error_msg = "Mocked Exception!"
    database_error = DBAPIError(statement="Mocked statement", params={}, orig=error_msg)

    return task_dao, create_request, database_error


def test_create_task_failure_retry_success(dbsession):
    task_dao, create_request, database_error = _prepare_for_retry_testcases(dbsession)

    when(dbsession).commit().thenRaise(database_error).thenRaise(
        database_error
    ).thenReturn(True)

    task_dao.create_task(create_request=create_request)

    num_of_annotations = _count_task(dbsession)
    assert num_of_annotations == 1


def test_upsert_annotation_failure_retry_exceeded(dbsession):
    task_dao, create_request, database_error = _prepare_for_retry_testcases(dbsession)

    when(dbsession).commit().thenRaise(database_error).thenRaise(
        database_error
    ).thenRaise(database_error)
    try:
        task_dao.create_task(create_request=create_request)
    except DBAPIError:
        pass

    num_of_annotations = _count_task(dbsession)
    assert num_of_annotations == 0


def test_upsert_annotation_failure_non_retriable_error(dbsession, monkeypatch):
    task_dao, create_request, database_error = _prepare_for_retry_testcases(dbsession)

    error_msg = "Mocked Exception!"

    def mock_commit():
        raise Exception(error_msg)

    monkeypatch.setattr(dbsession, "commit", mock_commit)

    try:
        task_dao.create_task(create_request=create_request)
    except Exception as e:
        assert str(e) == error_msg

    num_of_annotations = _count_task(dbsession)
    assert num_of_annotations == 0
