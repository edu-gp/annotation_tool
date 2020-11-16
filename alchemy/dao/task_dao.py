from sqlalchemy.exc import DBAPIError
from tenacity import (
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry,
)

from alchemy.data.request.task_request import TaskBaseRequest
from alchemy.db.model import Task


class TaskDao:
    def __init__(self, dbsession):
        self.dbsession = dbsession

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=6),
        retry=retry_if_exception_type(DBAPIError),
        reraise=True,
    )
    def create_task(self, create_request):
        if not create_request:
            # TODO We should consider adding Response Object in pair with the
            #  Request Object.
            raise Exception(f"Invalid request: {create_request.errors}")

        task = Task(name=create_request.name)

        self._check_duplicate_labels(list(set(create_request.labels)))
        self._configure_task_common(task, create_request)

        self.dbsession.add(task)
        self._commit_to_db()
        return task

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=6),
        retry=retry_if_exception_type(DBAPIError),
        reraise=True,
    )
    def update_task(self, update_request):
        if not update_request:
            # TODO We should consider adding Response Object in pair with the
            #  Request Object.
            raise Exception(f"Invalid request: {update_request.errors}")

        task = self.dbsession.query(Task).filter_by(id=update_request.id).one_or_none()

        if not task:
            raise ValueError(f"Invalid task id: {update_request.id}")

        self._check_duplicate_labels(
            new_labels=list(set(update_request.labels)), task_id_to_update=task.id
        )

        self._configure_task_common(task, update_request)

        self.dbsession.add(task)
        self._commit_to_db()
        return task

    def _configure_task_common(self, task: Task, request: TaskBaseRequest):
        labels = list(set(request.labels))
        annotators = list(set(request.annotators))
        task.name = request.name
        task.set_labels(labels)
        task.set_annotators(annotators)
        task.set_data_filenames(request.data_files)
        task.set_entity_type(request.entity_type)

    def _check_duplicate_labels(self, new_labels, task_id_to_update=None):
        """This is a temporary hacky solution to prevent users from creating
        duplicate labels. The proper solution involves a much bigger
        refactoring.

        Given the amount of tasks and labels we have, the performance is still
        acceptable.

        TODO replace this once we have the Market Management System in place.
        """
        if task_id_to_update:
            # We should only consider labels in tasks other than the one we are
            # trying to update in case we want to update the labels of a task from
            # ["label1"] to ["label1", "label2"]
            tasks = (
                self.dbsession.query(Task).filter(Task.id != task_id_to_update).all()
            )
        else:
            tasks = self.dbsession.query(Task).all()

        existing_labels = dict()
        for task in tasks:
            labels_in_task = task.get_labels()
            for label in labels_in_task:
                existing_labels[label] = task.name
        error_msgs = []
        for new_label in new_labels:
            if new_label in existing_labels:
                error_msgs.append(
                    f"Label {new_label} is already created "
                    f"in task {existing_labels[new_label]}"
                )
        if len(error_msgs) > 0:
            raise ValueError(error_msgs)

    def _commit_to_db(self):
        try:
            self.dbsession.commit()
        except Exception:
            self.dbsession.rollback()
            raise
