from tests.sqlalchemy_conftest import *
from db.model import (
    User, EntityType, Entity, Label, ClassificationAnnotation,
    ClassificationTrainingData,
    majority_vote_annotations_query
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


def _populate_db_manual(dbsession):
    # Create an EntityType
    ent = EntityType(name='Thing')
    dbsession.add(ent)
    dbsession.commit()

    # Create many Entities
    ents = [
        Entity(name=f'A', entity_type_id=ent.id),
        Entity(name=f'B', entity_type_id=ent.id),
        Entity(name=f'C', entity_type_id=ent.id),
        Entity(name=f'D', entity_type_id=ent.id),
        Entity(name=f'E', entity_type_id=ent.id),
    ]
    dbsession.add_all(ents)

    # Create many Users
    user = User(username=f'someuser')
    dbsession.add(user)

    dbsession.commit()

    # Create many Annotations for a Label
    label = Label(name='IsGood', entity_type_id=ent.id)

    def _create_anno(ent, v): return ClassificationAnnotation(
        entity=ent, user=user, label=label, value=v)

    annos = [
        _create_anno(ents[0], 1),
        _create_anno(ents[0], 1),
        _create_anno(ents[0], 1),
        _create_anno(ents[0], -1),
        _create_anno(ents[0], 0),
        _create_anno(ents[0], 0),
        _create_anno(ents[0], 0),
        _create_anno(ents[0], 0),

        _create_anno(ents[1], 1),
        _create_anno(ents[1], 1),
        _create_anno(ents[1], -1),
        _create_anno(ents[1], -1),
        _create_anno(ents[1], -1),
        _create_anno(ents[1], -1),
        _create_anno(ents[1], 0),
        _create_anno(ents[1], 0),
    ]

    dbsession.add_all([label] + annos)
    dbsession.commit()


def test_majority_vote_annotations_query(dbsession):
    _populate_db_manual(dbsession)
    label = dbsession.query(Label).first()

    query = majority_vote_annotations_query(dbsession, label)
    assert set(query.all()) == set([('A', 1, 3), ('B', -1, 4)])


def test_create_for_label(dbsession, monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))

    _populate_db_manual(dbsession)
    label = dbsession.query(Label).first()

    def entity_text_lookup_fn(entity_type_id, entity_name):
        return f'text for {entity_name}'

    data = ClassificationTrainingData.create_for_label(
        dbsession, label, entity_text_lookup_fn)

    data = data.load_data(to_df=False)
    data = sorted(data, key=lambda x: x['text'])
    assert data[0] == {'text': 'text for A', 'labels': {'IsGood': 1}}
    assert data[1] == {'text': 'text for B', 'labels': {'IsGood': -1}}


def _populate_db_variable(dbsession, n_users, n_entities):
    # Create a bunch of annotations

    # Create an EntityType
    ent = EntityType(name='Thing')
    dbsession.add(ent)
    dbsession.commit()

    # Create many Entities
    ents = [Entity(name=f'Thing{i}', entity_type_id=ent.id)
            for i in range(n_entities)]
    dbsession.add_all(ents)

    # Create many Users
    users = [User(username=f'someuser{u}')
             for u in range(n_users)]
    dbsession.add_all(users)

    dbsession.commit()

    # Create many Annotations for a Label
    label = Label(name='IsGood', entity_type_id=ent.id)
    annos = []
    for user in users:
        for i, ent in enumerate(ents):
            anno = ClassificationAnnotation(
                entity=ent, user=user, label=label,
                context={"text": f"abc{i}"},
                value=1 if i % 2 else -1)
            annos.append(anno)

    dbsession.add_all([label] + annos)
    dbsession.commit()


def test_create_for_label_load_test(dbsession, monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))

    # Load testing:
    # 10 users, 100 entities each =>
    #   Time to create data 0.3s
    #   Time to export data 0.006s
    # 10 users, 1000 entities each =>
    #   Time to create data 3.7s
    #   Time to export data 0.03s
    # 100 users, 1000 entities each =>
    #   Time to create data 33.3s
    #   Time to export data 0.07s

    import time
    st = time.time()

    n_users = 5
    n_entities = 10
    _populate_db_variable(dbsession, n_users=n_users, n_entities=n_entities)

    et = time.time()
    print("Time to create data", et-st)

    def entity_text_lookup_fn(entity_type_id, entity_name):
        return f'text for {entity_name}'

    label = dbsession.query(Label).first()
    data = ClassificationTrainingData.create_for_label(
        dbsession, label, entity_text_lookup_fn)

    print("Time to export data", time.time()-et)

    p = os.path.join(filestore_base_dir(), data.path())
    res = load_jsonl(p, to_df=False)
    assert len(res) == n_entities

    # Assert False at the end to see the print statements.
    # assert False
