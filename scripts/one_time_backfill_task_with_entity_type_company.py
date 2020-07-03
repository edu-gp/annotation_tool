import logging

from db.config import DevelopmentConfig
from db.model import Database, Task, EntityTypeEnum

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
    result = db.session.query(Task).all()

    for task in result:
        if task.get_entity_type() is None:
            task.set_entity_type(EntityTypeEnum.COMPANY)
            db.session.add(task)
    db.session.commit()
    db.session.close()
