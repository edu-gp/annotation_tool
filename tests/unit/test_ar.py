import ar
from ar import Pred

def test_assign_round_robin():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']

    per_anno_queue = ar._assign(datapoints, annotators, max_per_annotator=2, max_per_dp=1)

    assert per_anno_queue == {
        'u1': ['a', 'c'],
        'u2': ['b']
    }

def test_assign_unlimited_budget():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']

    per_anno_queue = ar._assign(datapoints, annotators, max_per_annotator=999, max_per_dp=999)

    assert per_anno_queue == {
        'u1': ['a', 'b', 'c'],
        'u2': ['a', 'b', 'c']
    }

def test_assign_limited_by_max_per_annotator():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']

    per_anno_queue = ar._assign(datapoints, annotators, max_per_annotator=2, max_per_dp=2)

    assert per_anno_queue == {
        'u1': ['a', 'b'],
        'u2': ['a', 'b']
    }

def test_assign_limited_by_max_per_dp():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']

    per_anno_queue = ar._assign(datapoints, annotators, max_per_annotator=2, max_per_dp=1)

    assert per_anno_queue == {
        'u1': ['a', 'c'],
        'u2': ['b']
    }

def test_assign_blacklist():
    datapoints = ['a', 'b', 'c']
    annotators = ['u1', 'u2']
    blacklist_fn = lambda dp, anno : (dp == 'a' and anno == 'u1')

    per_anno_queue = ar._assign(datapoints, annotators, max_per_annotator=999, max_per_dp=999,
                                blacklist_fn=blacklist_fn)

    assert per_anno_queue == {
        'u1': ['b', 'c'],
        'u2': ['a', 'b', 'c']
    }

def _do_test_shuffle(random_pred_class_a, random_pred_class_b, is_pred_class_a):
    # Pretend we have predictions from 2 models, we shuffle those 2 lists together.
    # Test that, at any reasonably length, the desired proportion of items from
    # each list should be evident.

    N = 1000 # Total number of data points
    M = 500  # Length at which we're going to check for proportions

    preds_a = [random_pred_class_a(i) for i in range(N)]
    preds_b = [random_pred_class_b(i) for i in range(N)]
    
    shuffled = ar._shuffle_together_examples([preds_a, preds_b], proportions=[0.8, 0.2])

    # Since there are N unique lines, the result should contain N elements.
    assert len(shuffled) == N

    # Check all different line_number's are maintained
    assert set([p.line_number for p in shuffled]), set(list(range(N)))

    print([1 if is_pred_class_a(pred) else 0 for pred in shuffled])
    n_first_class = sum([1 if is_pred_class_a(pred) else 0 for pred in shuffled[:M]])
    proportion_first_class = n_first_class / M
    
    # Empirically, 0.1 is about 6 * std, there's approx a 1 / 1,000,000,000 chance this test fails randomly.
    assert abs(proportion_first_class - 0.8) < 0.1

def test_shuffle():
    import random

    random_pred_class_a = lambda linenum : Pred(0.1 + random.random()/100, 'data.jsonl', linenum)
    random_pred_class_b = lambda linenum : Pred(0.9 + random.random()/100, 'data.jsonl', linenum)
    is_pred_class_a = lambda pred : pred.score < 0.5

    _do_test_shuffle(random_pred_class_a, random_pred_class_b, is_pred_class_a)
    
def test_shuffle_2():
    random_pred_class_a = lambda linenum : Pred(0.1, 'data.jsonl', linenum)
    random_pred_class_b = lambda linenum : Pred(0.9, 'data.jsonl', linenum)
    is_pred_class_a = lambda pred : pred.score < 0.5

    _do_test_shuffle(random_pred_class_a, random_pred_class_b, is_pred_class_a)
