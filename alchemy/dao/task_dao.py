from sqlalchemy.exc import DBAPIError
from tenacity import (
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    retry,
)

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
            raise Exception(f"Invalid request: {create_request.errors}")

        labels = list(set(create_request.labels))

        self._check_duplicate_labels(labels)

        annotators = list(set(create_request.annotators))
        task = Task(name=create_request.name)
        task.set_labels(labels)
        task.set_annotators(annotators)
        task.set_data_filenames(create_request.data_files)
        task.set_entity_type(create_request.entity_type)

        self.dbsession.add(task)
        self._commit_to_db()
        return task

    def _check_duplicate_labels(self, new_labels):
        """This is a temporary hacky solution to prevent users from creating
        duplicate labels. The proper solution involves a much bigger
        refactoring.

        Given the amount of tasks and labels we have, the performance is still
        acceptable.

        TODO replace this once we have the Market Management System in place.
        """
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

        raise Exception(error_msgs)

    def _commit_to_db(self):
        try:
            self.dbsession.commit()
        except Exception:
            self.dbsession.rollback()
            raise
