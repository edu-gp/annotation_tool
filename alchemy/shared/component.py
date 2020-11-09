from alchemy.dao.annotation_dao import AnnotationDao
from alchemy.dao.task_dao import TaskDao
from alchemy.db.model import db

annotation_dao = AnnotationDao(dbsession=db.session)
task_dao = TaskDao(dbsession=db.session)
