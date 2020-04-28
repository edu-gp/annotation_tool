from tests.sqlalchemy_conftest import *
from db.model import (
    EntityType, Entity, User,
    Label, AnnotationRequest, AnnotationType, AnnotationRequestStatus,
    ClassificationAnnotation,
)


def test_annotation_flow(dbsession):
    entity_type = EntityType(name='Fruit')
    user = User(username='Bob')
    dbsession.add_all([user, entity_type])
    dbsession.commit()

    # When we set up an annotation request, we create the following, as needed:
    label = Label(name="IsSweet", entity_type=entity_type)
    entity = Entity(name="Banana", entity_type=entity_type)
    req = AnnotationRequest(
        user=user, entity=entity, label=label, order=1,
        annotation_type=AnnotationType.ClassificationAnnotation,
        name="Is a Banana Sweet?", context={
            "text": "Studies have shown that banana is sweet when ripe."
        })
    dbsession.add_all([entity, label, req])
    dbsession.commit()

    # [INDEX] <- user_id
    # Bob logs in and sees that there is 1 request waiting for him.
    reqs = dbsession.query(AnnotationRequest).join(
        User).filter(AnnotationRequest.user == user).all()
    assert len(reqs) == 1

    # [SHOW] <- user_id, req_id
    # Bob clicks on the request, and sees the following:
    # "Hi Bob, you have 1 annotation request.""
    req = reqs[0]
    # "This request is called..."
    assert req.name == "Is a Banana Sweet?"
    # "We'd like your opinion on..."
    assert req.entity.name == "Banana"
    # "We'd like you to tell us..."
    assert req.label.name == "IsSweet"
    # "Here's the evidence we found..."
    assert len(req.context['text']) > 0
    # "You have no previous annotations on this"
    assert user.classification_annotations.filter_by(
        entity=req.entity).count() == 0

    # [RECEIVE_ANNOTATION] <- user_id, req_id, entity_name, label_name, value
    # Bob fulfills the request.
    anno = ClassificationAnnotation(
        label=label, value=1,
        user=user, entity=entity, context=req.context)
    # Note we copied the context over from the AnnotationRequest.
    dbsession.add(anno)
    # (!) Because this label is exactly the request's label, we also mark this
    # request as done.
    req.status = AnnotationRequestStatus.Complete
    dbsession.add(req)
    dbsession.commit()

    # [RECEIVE_ANNOTATION] <- user_id, req_id, entity_name, label_name, value
    # Bob suddenly remembers something else about Bananas.
    label_is_yellow = Label(name="IsYellow", entity_type=entity_type)
    anno = ClassificationAnnotation(
        label=label_is_yellow, value=1,
        user=user, entity=entity, context=req.context)
    # Note we copied the context over from the AnnotationRequest.
    dbsession.add(anno)
    dbsession.commit()

    # [SHOW] <- user_id, req_id
    # Later that day, Bob comes back to the page.
    # And sees that this request was already completed.
    assert req.status is AnnotationRequestStatus.Complete
    # And that he has 2 previous annotations on this item.
    assert user.classification_annotations.filter_by(
        entity=req.entity).count() == 2
    # Bob can now edit the annotations or create new ones.
