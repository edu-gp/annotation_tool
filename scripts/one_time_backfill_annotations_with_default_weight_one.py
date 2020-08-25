import logging

from db.config import DevelopmentConfig
from db.model import Database, Task, EntityTypeEnum, ClassificationAnnotation

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
    annotations = db.session.query(ClassificationAnnotation).all()

    for anno in annotations:
        if anno.weight is None:
            anno.weight = 1
            db.session.add(anno)
    db.session.commit()
    db.session.close()
