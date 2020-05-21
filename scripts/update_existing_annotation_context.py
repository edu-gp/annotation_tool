import logging

from db.config import DevelopmentConfig
from db.model import Database, ClassificationAnnotation, AnnotationRequest, \
    User

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
    annotations = db.session.query(ClassificationAnnotation).join(User).filter(
        User.username != 'salesforce_bot').all()
    entities = [annotation.entity for annotation in annotations]
    annotation_request_context_and_entity = db.session.\
        query(AnnotationRequest.context, AnnotationRequest.entity).\
        filter(AnnotationRequest.entity.in_(entities)).all()
    entity_to_context = {
        item[1]: item[0]
        for item in annotation_request_context_and_entity
    }

    for annotation in annotations:
        if annotation.entity in entity_to_context:
            logging.info("Updating Entity {}".format(annotation.entity))
            annotation.context = entity_to_context[annotation.entity]
        else:
            logging.info("Did not find request for entity {}".format(annotation.entity))

    db.session.add_all(annotations)
    db.session.commit()
    db.session.close()
