import logging

from alchemy.db.model import ClassificationAnnotation, Database

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    db = Database.bootstrap()
    annotations = db.session.query(ClassificationAnnotation).all()

    for anno in annotations:
        if anno.weight is None:
            anno.weight = 1
            db.session.add(anno)
    db.session.commit()
    db.session.close()
