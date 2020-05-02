import ar
from ar import Pred
from db.model import ClassificationAnnotation, get_or_create, User
from tests.sqlalchemy_conftest import *

def test_assign_round_robin():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']

    per_anno_queue = ar._assign(
        datapoints, annotators, max_per_annotator=2, max_per_dp=1)

    assert per_anno_queue == {
        'u1': ['a', 'c'],
        'u2': ['b']
    }


def test_assign_unlimited_budget():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']

    per_anno_queue = ar._assign(
        datapoints, annotators, max_per_annotator=999, max_per_dp=999)

    assert per_anno_queue == {
        'u1': ['a', 'b', 'c'],
        'u2': ['a', 'b', 'c']
    }


def test_assign_limited_by_max_per_annotator():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']

    per_anno_queue = ar._assign(
        datapoints, annotators, max_per_annotator=2, max_per_dp=2)

    assert per_anno_queue == {
        'u1': ['a', 'b'],
        'u2': ['a', 'b']
    }


def test_assign_limited_by_max_per_dp():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']

    per_anno_queue = ar._assign(
        datapoints, annotators, max_per_annotator=2, max_per_dp=1)

    assert per_anno_queue == {
        'u1': ['a', 'c'],
        'u2': ['b']
    }


def test_assign_blacklist():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']
    def blacklist_fn(dp, anno): return (dp == 'a' and anno == 'u1')

    per_anno_queue = ar._assign(
        datapoints, annotators, max_per_annotator=999, max_per_dp=999,
        blacklist_fn=blacklist_fn)

    assert per_anno_queue == {
        'u1': ['b', 'c'],
        'u2': ['a', 'b', 'c']
    }


def populate_db(dbsession):
    entity1 = "whatever1"
    entity2 = "whatever2"
    user1 = get_or_create(dbsession=dbsession,
                          model=User,
                          username="user1")
    annotation0 = get_or_create(dbsession=dbsession,
                                model=ClassificationAnnotation,
                                value=1,
                                entity=entity1,
                                entity_type="company",
                                label="b2c",
                                user_id=user1.id)
    annotation1 = get_or_create(dbsession=dbsession,
                                model=ClassificationAnnotation,
                                value=1,
                                entity=entity1,
                                entity_type="company",
                                label="healthcare",
                                user_id=user1.id)
    user2 = get_or_create(dbsession=dbsession,
                          model=User,
                          username="user2")
    annotation2 = get_or_create(dbsession=dbsession,
                                model=ClassificationAnnotation,
                                value=1,
                                entity=entity2,
                                entity_type="company",
                                label="b2c",
                                user_id=user2.id)
    annotations = [annotation0, annotation1, annotation2]
    return user1, user2, annotations, entity1, entity2


def test_build_blacklist_lookup_dict(dbsession):
    user1, user2, annotations, _, _ = populate_db(dbsession)
    lookup_dict = ar._build_blacklisting_lookup_dict(dbsession)
    assert lookup_dict == {
        ar.EntityUserTuple(annotations[0].entity, user1.username): 2,
        ar.EntityUserTuple(annotations[2].entity, user2.username): 1,
    }


def test_blacklisting_requests(dbsession):
    user1, user2, annotations, entity1, entity2 = populate_db(dbsession)
    dp1 = Pred(score=1, entity="new_entity", fname="fname", line_number=1)
    dp2 = Pred(score=1, entity=entity1, fname="fname", line_number=1)
    dp3 = Pred(score=1, entity=entity2, fname="fname", line_number=1)
    dp4 = Pred(score=1, entity="entity_new", fname="fname", line_number=1)
    datapoints = [dp1, dp2, dp3, dp4]
    annotators = [user1.username, user2.username]
    lookup_dict = ar._build_blacklisting_lookup_dict(dbsession=dbsession)
    blacklist_fn = ar._build_blacklist_fn(lookup_dict=lookup_dict)

    per_anno_queue = ar._assign(
        datapoints, annotators, max_per_annotator=999, max_per_dp=999,
        blacklist_fn=blacklist_fn)

    assert per_anno_queue == {
        user1.username: [dp1, dp3, dp4],
        user2.username: [dp1, dp2, dp4]
    }


def _do_test_shuffle(random_pred_class_a, random_pred_class_b, is_pred_class_a):
    # Pretend we have predictions from 2 models, we shuffle those 2 lists together.
    # Test that, at any reasonably length, the desired proportion of items from
    # each list should be evident.

    N = 1000  # Total number of data points
    M = 500  # Length at which we're going to check for proportions

    preds_a = [random_pred_class_a(i) for i in range(N)]
    preds_b = [random_pred_class_b(i) for i in range(N)]

    shuffled = ar._shuffle_together_examples(
        [preds_a, preds_b], proportions=[0.8, 0.2])

    # Since there are N unique lines, the result should contain N elements.
    assert len(shuffled) == N

    # Check all different line_number's are maintained
    assert set([p.line_number for p in shuffled]), set(list(range(N)))

    print([1 if is_pred_class_a(pred) else 0 for pred in shuffled])
    n_first_class = sum([1 if is_pred_class_a(
        pred) else 0 for pred in shuffled[:M]])
    proportion_first_class = n_first_class / M

    # Empirically, 0.1 is about 6 * std, there's approx a 1 / 1,000,000,000 chance this test fails randomly.
    assert abs(proportion_first_class - 0.8) < 0.1


def test_shuffle():
    import random

    def random_pred_class_a(linenum): return ar.Pred(
        score=0.1 + random.random()/100,
        entity=str(linenum) + ".com",
        fname='data.jsonl',
        line_number=linenum)

    def random_pred_class_b(linenum): return ar.Pred(
        score=0.9 + random.random()/100,
        entity=str(linenum) + ".com",
        fname='data.jsonl',
        line_number=linenum)

    def is_pred_class_a(pred): return pred.score < 0.5

    _do_test_shuffle(random_pred_class_a, random_pred_class_b, is_pred_class_a)


def test_shuffle_2():
    def random_pred_class_a(linenum): return ar.Pred(
        score=0.1,
        entity=str(linenum) + ".com",
        fname='data.jsonl', line_number=linenum)

    def random_pred_class_b(linenum): return ar.Pred(
        score=0.9,
        entity=str(linenum) + ".com",
        fname='data.jsonl',
        line_number=linenum)

    def is_pred_class_a(pred): return pred.score < 0.5

    _do_test_shuffle(random_pred_class_a, random_pred_class_b, is_pred_class_a)
