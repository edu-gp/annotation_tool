import pytest
from mockito import when
from sqlalchemy.exc import DBAPIError

from alchemy.dao.task_dao import TaskDao
from alchemy.data.request.task_request import TaskCreateRequest, TaskUpdateRequest
from alchemy.db.model import EntityTypeEnum, Task

entity_type = EntityTypeEnum.COMPANY
name = "b2c"
labels = ["hotdog"]
annotators = ["user1", "user2"]
data_files = ["file1.txt", "file2.txt"]

entity_type_updated = "future_type"
name_updated = "bbb"
labels_updated = ["healthcare"]
annotators_updated = annotators + ["user3"]
data_files_updated = data_files + ["file3.txt"]


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


def _prepare_existing_task(dbsession):
    existing_task = Task(name=name)
    existing_task.set_labels(labels)
    existing_task.set_data_filenames(data_files)
    existing_task.set_entity_type(EntityTypeEnum.COMPANY)
    existing_task.set_annotators(annotators)

    dbsession.add(existing_task)
    dbsession.commit()
    return existing_task


@pytest.mark.parametrize("new_labels", [labels_updated, labels + ["new_label"]])
def test_task_update_success(dbsession, new_labels):
    existing_task = _prepare_existing_task(dbsession)

    task_dao = TaskDao(dbsession=dbsession)
    update_request = TaskUpdateRequest(
        task_id=existing_task.id,
        name=name_updated,
        entity_type=entity_type_updated,
        labels=new_labels,
        annotators=annotators_updated,
        data_files=data_files,
    )

    updated_task = task_dao.update_task(update_request=update_request)

    num_of_tasks = _count_task(dbsession)
    assert num_of_tasks == 1

    assert updated_task is not None
    assert updated_task.id == existing_task.id
    assert updated_task.name == name_updated
    assert set(updated_task.get_labels()) == set(new_labels)
    assert set(updated_task.get_annotators()) == set(annotators_updated)
    assert set(updated_task.get_data_filenames()) == set(data_files)


def test_task_create_with_invalid_request(dbsession):
    task_dao = TaskDao(dbsession=dbsession)
    create_request = TaskCreateRequest.from_dict({"name": name})

    try:
        _ = task_dao.create_task(create_request=create_request)
    except Exception as e:
        assert "Invalid request:" in str(e)

    num_of_tasks = _count_task(dbsession)
    assert num_of_tasks == 0


def test_task_update_with_invalid_request(dbsession):
    existing_task = _prepare_existing_task(dbsession)

    task_dao = TaskDao(dbsession=dbsession)
    update_request = TaskUpdateRequest.from_dict(
        {"id": existing_task.id, "name": name + "_test"}
    )

    try:
        _ = task_dao.update_task(update_request=update_request)
    except Exception as e:
        assert "Invalid request:" in str(e)

    assert existing_task.name == name
    assert set(existing_task.get_labels()) == set(labels)
    assert set(existing_task.get_data_filenames()) == set(data_files)
    assert set(existing_task.get_annotators()) == set(annotators)
    assert existing_task.get_entity_type() == entity_type


def test_task_create_with_duplicate_labels(dbsession):
    existing_task = _prepare_existing_task(dbsession)

    task_dao = TaskDao(dbsession=dbsession)
    create_request = TaskCreateRequest(
        name=name,
        entity_type=entity_type,
        labels=labels + ["new_label"],
        annotators=annotators,
        data_files=data_files,
    )

    expected_message = (
        f"Label {labels[0]} is already created in task {existing_task.name}"
    )
    with pytest.raises(ValueError) as e:
        _ = task_dao.create_task(create_request=create_request)
    assert expected_message in str(e.value)

    num_of_tasks = _count_task(dbsession)
    assert num_of_tasks == 1

    task = dbsession.query(Task).one_or_none()

    assert existing_task.id == task.id


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
