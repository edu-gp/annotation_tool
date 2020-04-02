import os
from db.task import Task
from ar.data import (
    save_new_ar_for_user,
    get_ar_id,
    compute_annotation_statistics,
    fetch_tasks_for_user,
    fetch_all_ar,
    fetch_ar,
    fetch_all_annotations,
    fetch_annotation,
    annotate_ar,
    get_next_ar,
    export_labeled_examples,
)

# TODO test the actual saving and deleting of Task

# TODO
# monkeypatch.setenv('ANNOTATION_TOOL_TASKS_DIR', '/tmp/__tasks')
# monkeypatch.setenv('ANNOTATION_TOOL_DATA_DIR', '/tmp/__data')
# monkeypatch.setenv('ANNOTATION_TOOL_INFERENCE_CACHE_DIR', '/tmp/__infcache')


class TestARFlow:
    def setup_method(self, test_method):
        task = Task('just testing ar')
        task.task_id = '__testing_'+task.task_id
        task.save()
        self.task = task

    def teardown_method(self, test_method):
        # Make sure we delete the task even when the test fails.
        self.task.delete()

    def test_ar_flow(self):
        task = self.task
        task_id = task.task_id
        user_id = 'eddie_test'

        # - New Annotation Requests.
        annotation_requests = [
            {
                'ar_id': get_ar_id('a', 1),
                'fname': 'a',
                'line_number': 1,

                'score': 0.9,
                'data': {'text': 'blah', 'meta': {'foo': 'bar'}}
            },
            {
                'ar_id': get_ar_id('a', 2),
                'fname': 'a',
                'line_number': 2,

                'score': 0.5,
                'data': {'text': 'blah', 'meta': {'foo': 'bar'}}
            },
            {
                'ar_id': get_ar_id('a', 3),
                'fname': 'a',
                'line_number': 3,

                'score': 0.1,
                'data': {'text': 'blah', 'meta': {'foo': 'bar'}}
            },
        ]

        basedir = save_new_ar_for_user(task_id, user_id, annotation_requests)
        assert len(annotation_requests) == len(os.listdir(basedir))

        # Check we can compute stats properly
        stats = compute_annotation_statistics(task_id)
        assert stats['total_annotations'] == 0
        assert sum(stats['n_annotations_per_user'].values()
                   ) == 0  # nothing annotated yet
        assert sum(stats['n_annotations_per_label'].values()
                   ) == 0  # nothing annotated yet
        assert stats['total_outstanding_requests'] == 3
        assert stats['n_outstanding_requests_per_user'][user_id] == 3
        # print(stats)

        user_task_ids = fetch_tasks_for_user(user_id)
        assert len(user_task_ids) > 0
        assert task_id in user_task_ids

        all_ars = fetch_all_ar(task_id, user_id)
        assert len(all_ars) == len(annotation_requests)

        # Make sure order is right.
        assert annotation_requests[0]['ar_id'] == all_ars[0]
        assert annotation_requests[1]['ar_id'] == all_ars[1]
        assert annotation_requests[2]['ar_id'] == all_ars[2]

        # User has not labeled anything yet.
        assert len(fetch_all_annotations(task_id, user_id)) == 0

        ar_detail = fetch_ar(task_id, user_id, all_ars[0])
        assert ar_detail['ar_id'] == annotation_requests[0]['ar_id']
        assert ar_detail['score'] == annotation_requests[0]['score']

        # Annotate an existing thing
        my_anno = {'labels': {'HEALTHCARE': 1}}
        annotate_ar(task_id, user_id, all_ars[0], my_anno)
        assert fetch_annotation(task_id, user_id, all_ars[0])[
            'anno'] == my_anno
        assert len(fetch_all_annotations(task_id, user_id)) == 1

        # Annotate the same thing again updates the annotation
        updated_anno = {'labels': {'FINTECH': 1, 'HEALTHCARE': 0}}
        annotate_ar(task_id, user_id, all_ars[0], updated_anno)
        assert fetch_annotation(task_id, user_id, all_ars[0])[
            'anno'] == updated_anno
        assert len(fetch_all_annotations(task_id, user_id)) == 1

        # Annotate something that doesn't exist
        # (this might happen when master is updating - we'll try to avoid it)
        annotate_ar(task_id, user_id, 'doesnotexist',
                    {'labels': {'HEALTHCARE': 1}})
        assert len(fetch_all_annotations(task_id, user_id)) == 1

        # Fetch next thing to be annotated
        # ar[0] is labeled, label ar[1]
        assert get_next_ar(task_id, user_id, all_ars[0]) == all_ars[1]
        # user skipped ar[1] to ar[2]
        assert get_next_ar(task_id, user_id, all_ars[1]) == all_ars[2]
        # user skipped ar[2], next unlabeled point is ar[1]
        assert get_next_ar(task_id, user_id, all_ars[2]) == all_ars[1]

        # New batch of data to be annotated (check it purges the previous ones)
        new_annotation_requests = [
            {
                'ar_id': get_ar_id('a', 10),
                'fname': 'a',
                'line_number': 10,

                'score': 0.9,
                'data': {'text': 'blah', 'meta': {'foo': 'bar'}}
            },
            {
                'ar_id': get_ar_id('a', 11),
                'fname': 'a',
                'line_number': 11,

                'score': 0.9,
                'data': {'text': 'loremipsum', 'meta': {'foo': '123xyz'}}
            },
            {
                'ar_id': get_ar_id('a', 12),
                'fname': 'a',
                'line_number': 12,

                'score': 0.9,
                'data': {'text': 'loremipsum', 'meta': {'foo': '123xyz'}}
            },
            {
                'ar_id': get_ar_id('a', 13),
                'fname': 'a',
                'line_number': 13,

                'score': 0.9,
                'data': {'text': 'loremipsum', 'meta': {'foo': '123xyz'}}
            }
        ]

        basedir = save_new_ar_for_user(
            task_id, user_id, new_annotation_requests)
        assert len(new_annotation_requests) == len(os.listdir(basedir))
        assert len(new_annotation_requests) == len(
            fetch_all_ar(task_id, user_id))

        # Check existing annotations still exist
        assert len(fetch_all_annotations(task_id, user_id)) == 1

        # Annotate something thing in the new batch
        my_anno = {'labels': {'MACHINELEARNING': 0,
                              'FINTECH': 1, 'HEALTHCARE': -1}}
        all_ars = fetch_all_ar(task_id, user_id)
        annotate_ar(task_id, user_id, all_ars[0], my_anno)
        assert fetch_annotation(task_id, user_id, all_ars[0])[
            'anno'] == my_anno
        assert len(fetch_all_annotations(task_id, user_id)) == 2

        # TODO: Write the test. When there is 1 thing left in queue, get_next_ar should return None.
        # assert get_next_ar(task_id, user_id, all_ars[-1]) == None

        # Check we can compute stats properly
        stats = compute_annotation_statistics(task_id)
        # print(stats)
        assert stats['total_annotations'] == 2
        assert stats['n_annotations_per_user'][user_id] == 2
        assert stats['n_annotations_per_label']['FINTECH'] == {1: 2}
        assert stats['n_annotations_per_label']['HEALTHCARE'] == {0: 1, -1: 1}
        assert stats['n_annotations_per_label']['MACHINELEARNING'] == {0: 1}
        assert stats['total_outstanding_requests'] == 3
        assert stats['n_outstanding_requests_per_user'][user_id] == 3

        assert sum(stats['n_outstanding_requests_per_user'].values()
                   ) == stats['total_outstanding_requests']

        # Annotate one thing that we're unsure of
        my_anno = {'labels': {'MACHINELEARNING': 0}}
        all_ars = fetch_all_ar(task_id, user_id)
        annotate_ar(task_id, user_id, all_ars[1], my_anno)

        # Try exporting
        # (+ Make sure it didn't include the unsure annotation we just made)
        exported = export_labeled_examples(task_id)
        assert len(exported) == 2
        assert {'text': 'blah', 'labels': {'FINTECH': 1}} in exported
        assert {'text': 'blah', 'labels': {
            'FINTECH': 1, 'HEALTHCARE': -1}} in exported
