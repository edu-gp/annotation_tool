import logging

from sqlalchemy.exc import DBAPIError
from tenacity import (
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry,
)

from alchemy.data.request.task_request import TaskBaseRequest, TaskUpdateRequest
from alchemy.db.model import (
    Task,
    # TODO these functions should be moved outside of the model module.
    delete_requests_under_task,
    delete_requests_for_entity_type_under_task,
    delete_requests_for_user_under_task,
    delete_requests_for_label_under_task,
)


def _configure_task_common(task: Task, request: TaskBaseRequest):
    labels = list(set(request.labels))
    annotators = list(set(request.annotators))
    task.name = request.name
    task.set_labels(labels)
    task.set_annotators(annotators)
    task.set_data_filenames(request.data_files)
    task.set_entity_type(request.entity_type)


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
        _configure_task_common(task, create_request)

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

        self._remove_obsolete_requests_under_task(update_request, task)

        _configure_task_common(task, update_request)

        self.dbsession.add(task)
        self._commit_to_db()
        return task

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

    # TODO legacy code we need to refactor but have to keep it for now.
    def _remove_obsolete_requests_under_task(
        self, update_request: TaskUpdateRequest, task_to_update: Task
    ):
        if set(update_request.data_files) != set(task_to_update.get_data_filenames()):
            logging.info(
                "Prepare to remove all requests under task {} "
                "since the data file has changed".format(task_to_update.id)
            )
            delete_requests_under_task(self.dbsession, task_to_update.id)
        elif update_request.entity_type != task_to_update.get_entity_type():
            logging.info(
                "Prepare to remove all requests under task {} "
                "since the entity type has changed".format(task_to_update.id)
            )
            delete_requests_for_entity_type_under_task(
                self.dbsession, task_to_update.id, update_request.entity_type
            )
        else:
            # Updating the annotators
            for current_annotator in task_to_update.get_annotators():
                if current_annotator not in update_request.annotators:
                    logging.info(
                        "Prepare to remove requests under user {} for "
                        "task {}".format(current_annotator, task_to_update.id)
                    )
                    delete_requests_for_user_under_task(
                        self.dbsession, current_annotator, task_to_update.id
                    )
            # Updating the labels
            for current_label in task_to_update.get_labels():
                if current_label not in update_request.labels:
                    logging.info(
                        "Prepare to remove requests under label {} for "
                        "task {}".format(current_label, task_to_update.id)
                    )
                    delete_requests_for_label_under_task(
                        self.dbsession, current_label, task_to_update.id
                    )

    def _commit_to_db(self):
        try:
            self.dbsession.commit()
        except Exception:
            self.dbsession.rollback()
            raise
