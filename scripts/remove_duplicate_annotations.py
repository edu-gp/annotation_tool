import logging
from collections import defaultdict

from sqlalchemy import distinct, func
from sqlalchemy.exc import DatabaseError

from db.config import DevelopmentConfig
from db.model import Database, ClassificationAnnotation, AnnotationValue

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)

    distinct_labels = db.session.query(
        distinct(ClassificationAnnotation.label)).all()

    for label in distinct_labels:
        logging.info(f"Processing label {label}...")
        res = db.session.query(
            ClassificationAnnotation.entity,
            ClassificationAnnotation.user_id,
            func.count(ClassificationAnnotation.id)
        ).filter(ClassificationAnnotation.label == label)\
            .group_by(
            ClassificationAnnotation.user_id,
            ClassificationAnnotation.entity).all()

        for entity, user_id, num in res:
            if num > 1:
                logging.info(f"Found duplicates for entity {entity} "
                             f"under label {label} and user {user_id}...")
                duplicates = db.session.query(ClassificationAnnotation).filter(
                    ClassificationAnnotation.entity == entity,
                    ClassificationAnnotation.user_id == user_id,
                    ClassificationAnnotation.label == label
                ).all()

                logging.info(duplicates)

                value_count = defaultdict(int)
                for anno in duplicates:
                    value_count[anno.value] += 1
                logging.info(f"annotation value ratio is {value_count}")

                # keep the last annotation and delete the rest
                for anno in duplicates[:-1]:
                    db.session.delete(anno)

                # if there are different annotation values, reset to 0
                if len(value_count) > 1:
                    duplicates[-1].value = AnnotationValue.UNSURE
                    db.session.add(duplicates[-1])

                try:
                    db.session.commit()
                except DatabaseError as e:
                    logging.error(e)
                    db.session.rollback()
                    raise e




