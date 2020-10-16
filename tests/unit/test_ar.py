from alchemy import ar
from alchemy.ar import Example
from alchemy.db.model import (
    ClassificationAnnotation, get_or_create, User, LabelPatterns
)


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
    users = [
        get_or_create(dbsession=dbsession, model=User, username='user0'),
        get_or_create(dbsession=dbsession, model=User, username='user1'),
    ]
    entities = [
        'entity0',
        'entity1',
    ]

    def create_anno(user: User, entity: str, label: str, value: int):
        instance = ClassificationAnnotation(
            entity_type="blah",
            entity=entity,
            label=label,
            value=value,
            user_id=user.id
        )
        dbsession.add(instance)
        dbsession.commit()

    annotations = [
        # Agreement
        create_anno(users[0], entities[0], 'foo', 1),
        create_anno(users[1], entities[0], 'foo', 1),

        # Disagreement
        create_anno(users[0], entities[1], 'foo', 1),
        create_anno(users[1], entities[1], 'foo', -1),

        # Duplicate
        create_anno(users[0], entities[0], 'bar', 1),
        create_anno(users[0], entities[0], 'bar', 1),
    ]

    return users, entities, annotations


def test_build_blacklist_lookup(dbsession):
    populate_db(dbsession)

    def key(entity, label, user):
        return ar.LookupKey(entity_type='blah', entity=entity, label=label, user=user)

    lookup = ar._build_blacklist_lookup(dbsession, ['foo'])
    foo_lookup = set({
        key('entity0', 'foo', 'user0'),
        key('entity0', 'foo', 'user1'),
        key('entity1', 'foo', 'user0'),
        key('entity1', 'foo', 'user1')
    })
    print(lookup)
    print(foo_lookup)
    assert lookup == foo_lookup

    lookup = ar._build_blacklist_lookup(dbsession, ['bar'])
    bar_lookup = set({
        key('entity0', 'bar', 'user0')
    })
    assert lookup == bar_lookup

    lookup = ar._build_blacklist_lookup(dbsession, ['foo', 'bar'])
    assert lookup == foo_lookup.union(bar_lookup)


def test_blacklisting_requests(dbsession):
    users, entities, annotations = populate_db(dbsession)

    def create_dummy_example(entity, label):
        return Example(score=1, entity_type="blah", entity=entity, label=label,
                       fname="fname", line_number=1)

    # Both users have annotated these entities with label='foo',
    # So they should not be assigned again.
    ex0 = create_dummy_example(entities[0], 'foo')
    ex1 = create_dummy_example(entities[1], 'foo')

    # Only users[0] has annotated entities[0] with label='bar',
    # So it should not be assigned to users[0] again.
    ex2 = create_dummy_example(entities[0], 'bar')
    ex3 = create_dummy_example(entities[1], 'bar')

    # New entity that has not been annotated by anyone yet.
    ex4 = create_dummy_example('new_entity', 'foo')
    ex5 = create_dummy_example('new_entity', 'bar')

    examples = [ex0, ex1, ex2, ex3, ex4, ex5]

    annotators = [user.username for user in users]
    lookup = ar._build_blacklist_lookup(dbsession, ['foo', 'bar'])
    blacklist_fn = ar._build_blacklist_fn(lookup)

    per_anno_queue = ar._assign(
        examples, annotators, max_per_annotator=999, max_per_dp=999,
        blacklist_fn=blacklist_fn)

    assert per_anno_queue == {
        users[0].username: [ex3, ex4, ex5],
        users[1].username: [ex2, ex3, ex4, ex5]
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

    def random_pred_class_a(linenum):
        return Example(
            score=0.1 + random.random()/100,
            entity_type="blah",
            entity=str(linenum) + ".com",
            label="foo",
            fname='data.jsonl',
            line_number=linenum)

    def random_pred_class_b(linenum):
        return Example(
            score=0.9 + random.random()/100,
            entity_type="blah",
            entity=str(linenum) + ".com",
            label="foo",
            fname='data.jsonl',
            line_number=linenum)

    def is_pred_class_a(pred): return pred.score < 0.5

    _do_test_shuffle(random_pred_class_a, random_pred_class_b, is_pred_class_a)


def test_shuffle_2():
    def random_pred_class_a(linenum):
        return Example(
            score=0.1,
            entity_type="blah",
            entity=str(linenum) + ".com",
            label="foo",
            fname='data.jsonl', line_number=linenum)

    def random_pred_class_b(linenum):
        return Example(
            score=0.9,
            entity_type="blah",
            entity=str(linenum) + ".com",
            label="foo",
            fname='data.jsonl',
            line_number=linenum)

    def is_pred_class_a(pred): return pred.score < 0.5

    _do_test_shuffle(random_pred_class_a, random_pred_class_b, is_pred_class_a)


def test__consolidate_ranked_examples_per_label__simple():
    def ex(entity):
        return Example(score=0, entity_type="blah", entity=entity,
                       label="foo", fname='x', line_number=1)

    # All labels assigned highest scores to 'c', then 'b', then 'a'.
    ranked_examples_per_label = [
        [ex('a'), ex('b'), ex('c')],
        [ex('a'), ex('b'), ex('c')],
    ]

    res = ar.consolidate_ranked_examples_per_label(ranked_examples_per_label)
    res = [x.entity for x in res]
    assert res == ['a', 'b', 'c']


def test__consolidate_ranked_examples_per_label__round_robin():
    def ex(entity):
        return Example(score=0, entity_type="blah", entity=entity,
                       label="foo", fname='x', line_number=1)

    # Round robin algo looks at the 1st column (a, a, c),
    # then 2nd col (b, c, a), and so on.
    ranked_examples_per_label = [
        [ex('a'), ex('b'), ex('c')],
        [ex('a'), ex('c'), ex('b')],
        [ex('c'), ex('a'), ex('b')],
    ]

    res = ar.consolidate_ranked_examples_per_label(ranked_examples_per_label)
    res = [x.entity for x in res]
    assert res == ['a', 'c', 'b']


def test__get_pattern_model_for_label__bad_label(dbsession):
    pat_model = ar.get_pattern_model_for_label(dbsession, 'bad_label')
    assert pat_model is None


def test__get_pattern_model_for_label__no_pat(dbsession):
    pat = LabelPatterns(label='test')
    pat.set_positive_patterns([])
    dbsession.add(pat)
    dbsession.commit()

    pat_model = ar.get_pattern_model_for_label(dbsession, 'test')
    assert pat_model is None


def test__get_pattern_model_for_label__normal(dbsession):
    pat = LabelPatterns(label='test')
    pat.set_positive_patterns(['hello world', 'abc'])
    dbsession.add(pat)
    dbsession.commit()

    pat_model = ar.get_pattern_model_for_label(dbsession, 'test')
    assert pat_model is not None

    # The first sentence matches a pattern, the second doesn't.
    res = pat_model.predict(['blah blah hello world', 'xyz'])
    assert res[0]['score'] > 0
    assert res[1]['score'] == 0
