import os

from alchemy.db.fs import filestore_base_dir
from alchemy.db.model import ClassificationAnnotation, ClassificationTrainingData, User
from alchemy.shared.utils import load_jsonl

ENTITY_TYPE = "Foo"
LABEL = "MyLabel"


def test_path(dbsession):
    data = ClassificationTrainingData(label=LABEL)
    dbsession.add(data)
    dbsession.commit()
    dbsession.refresh(data)

    assert data.path() is not None
    assert data.path().endswith(".jsonl")


def _populate_db_manual(dbsession, weight=1):
    user = User(username=f"someuser")
    dbsession.add(user)
    dbsession.commit()

    def _create_anno(ent, v, weight=1):
        return ClassificationAnnotation(
            entity_type=ENTITY_TYPE,
            entity=ent,
            user=user,
            label=LABEL,
            value=v,
            weight=weight,
        )

    ents = ["A", "B"]

    annos = [
        _create_anno(ents[0], 1),
        _create_anno(ents[0], 1),
        _create_anno(ents[0], 1),
        _create_anno(ents[0], -1, weight=weight),
        _create_anno(ents[0], 0),
        _create_anno(ents[0], 0),
        _create_anno(ents[0], 0),
        _create_anno(ents[0], 0),
        _create_anno(ents[1], 1),
        _create_anno(ents[1], 1, weight=weight),
        _create_anno(ents[1], -1),
        _create_anno(ents[1], -1),
        _create_anno(ents[1], -1),
        _create_anno(ents[1], -1),
        _create_anno(ents[1], 0),
        _create_anno(ents[1], 0),
    ]

    dbsession.add_all(annos)
    dbsession.commit()


def test_create_for_label(dbsession):
    _populate_db_manual(dbsession)

    def entity_text_lookup_fn(entity_type_id, entity_name):
        return f"text for {entity_name}"

    data = ClassificationTrainingData.create_for_label(
        dbsession, ENTITY_TYPE, LABEL, entity_text_lookup_fn
    )

    data = data.load_data(to_df=False)
    data = sorted(data, key=lambda x: x["text"])
    assert data[0] == {"text": "text for A", "labels": {LABEL: 1}}
    assert data[1] == {"text": "text for B", "labels": {LABEL: -1}}


def _populate_db_variable(dbsession, n_users, n_entities):
    ents = [f"Thing{i}" for i in range(n_entities)]

    # Create many Users
    users = [User(username=f"someuser{u}") for u in range(n_users)]
    dbsession.add_all(users)
    dbsession.commit()

    # Create many Annotations for a Label
    # label = Label(name='IsGood', entity_type_id=ent.id)
    annos = []
    for user in users:
        for i, ent in enumerate(ents):
            anno = ClassificationAnnotation(
                entity_type=ENTITY_TYPE,
                entity=ent,
                user=user,
                label=LABEL,
                context={"text": f"abc{i}"},
                value=1 if i % 2 else -1,
            )
            annos.append(anno)

    dbsession.add_all(annos)
    dbsession.commit()


def test_create_for_label_load_test(dbsession):
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
    print("Time to create data", et - st)

    def entity_text_lookup_fn(entity_type_id, entity_name):
        return f"text for {entity_name}"

    data = ClassificationTrainingData.create_for_label(
        dbsession, ENTITY_TYPE, LABEL, entity_text_lookup_fn
    )

    print("Time to export data", time.time() - et)

    p = os.path.join(filestore_base_dir(), data.path())
    res = load_jsonl(p, to_df=False)
    assert len(res) == n_entities

    # Assert False at the end to see the print statements.
    # assert False
