from tests.sqlalchemy_conftest import *
from db.model import (
    User, EntityType, Entity, Label, Context, ClassificationAnnotation,
    ClassificationTrainingData
)
import os
from db.fs import filestore_base_dir
from shared.utils import load_jsonl


def test_path(dbsession):
    label = Label(name='hello')
    data = ClassificationTrainingData(label=label)
    dbsession.add_all([data, label])
    dbsession.commit()
    dbsession.refresh(data)

    assert data.path() is not None
    assert data.path().endswith('.jsonl')


def _populate_db(dbsession, n_users, n_entities):
    # Create a bunch of annotations

    # Create an EntityType
    ent = EntityType(name='Person')
    dbsession.add(ent)
    dbsession.commit()

    # Create many Entities
    ents = [Entity(name=f'Person{i}', entity_type_id=ent.id)
            for i in range(n_entities)]
    dbsession.add_all(ents)

    # Create many Users
    users = [User(username=f'someuser{u}')
             for u in range(n_users)]
    dbsession.add_all(users)

    dbsession.commit()

    # Create many Annotations for a Label "tall"
    label = Label(name='tall', entity_type_id=ent.id)
    annos = []
    for user in users:
        for i, ent in enumerate(ents):
            ctx = Context.get_or_create(dbsession, {"text": f"abc{i}"})
            anno = ClassificationAnnotation(
                entity=ent, user=user, label=label, context=ctx,
                value=1 if i % 2 else -1)
            annos.append(anno)

    dbsession.add_all([label] + annos)
    dbsession.commit()


def test_create(dbsession, monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', tmp_path)

    # Load testing:
    # 10 users, 100 entities each =>
    #   Time to create data 2s
    #   Time to export data .6s
    # 10 users, 1000 entities each =>
    #   Time to create data 24s
    #   Time to export data 59s

    import time
    st = time.time()

    n_users = 5
    n_entities = 10
    _populate_db(dbsession, n_users=n_users, n_entities=n_entities)

    et = time.time()
    print("Time to create data", et-st)

    label = dbsession.query(Label).first()
    data = ClassificationTrainingData.create_for_label(dbsession, label)

    print("Time to export data", time.time()-et)

    p = os.path.join(filestore_base_dir(), data.path())
    res = load_jsonl(p, to_df=False)
    assert len(res) == n_entities
