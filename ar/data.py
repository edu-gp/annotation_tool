import os
import shutil
import re
import glob

from shared.utils import load_json, save_json, mkf, mkd
from db.task import Task, DIR_ANNO, DIR_AREQ
from db import _task_dir

from .utils import get_ar_id

###############################################################################
# I chose to write it all on disk for now - we can change it to a db later.

def save_new_ar_for_user(task_id, user_id, annotation_requests, clean_existing=True):
    '''
    Save a list of new annotation requests for a user (annotator).
    Args:
        task_id: -
        user_id: -
        annotation_requests: A list of dict objects
        clean_existing: Optionally erase all existing requests
    '''
    # Save each file individually for fast random access.

    # TODO save to Redis?
    task = Task.fetch(task_id)

    basedir = [task.get_dir(), DIR_AREQ, str(user_id)]
    _basedir = os.path.join(*basedir)

    if clean_existing:
        # Clean out all old requests first
        if os.path.isdir(_basedir):
            shutil.rmtree(_basedir)
    
    mkd(*basedir)

    # NOTE insert these in reverse order so the most recently created ones are
    # the ones to be labeled first.
    annotation_requests = annotation_requests[::-1]

    # Save each annotation request request as a single file.
    for req in annotation_requests:
        path = basedir + [f"{req['ar_id']}.json"]
        fname = os.path.join(*path)
        save_json(fname, req)

    # Return the dir holding all the AR's.
    return _basedir

def fetch_tasks_for_user(user_id):
    '''
    Return a list of task_id for which the user has annotation jobs to do.
    '''
    fnames = glob.glob(os.path.join(_task_dir(), '*', DIR_AREQ, user_id))
    task_ids = [re.match(f'^{_task_dir()}/(.*)/{DIR_AREQ}.*$', f).groups()[0]
                for f in fnames]
    return task_ids

def fetch_all_ar(task_id, user_id):
    '''
    Return a list of ar_id for this task
    '''
    _dir = os.path.join(_task_dir(task_id), DIR_AREQ, user_id)
    return _get_all_ar_ids_in_dir(_dir, sort_by_ctime=True)

def fetch_ar(task_id, user_id, ar_id):
    '''
    Return the details of a annotation request
    '''
    fname = os.path.join(_task_dir(task_id), DIR_AREQ, user_id, ar_id + '.json')
    return load_json(fname)

def get_next_ar(task_id, user_id, ar_id):
    '''
    Get the next ar_id to show to user, or None.
    This function should be very fast.

    Logic:
        - If ar_id exist in list, get the next one that has not been labeled.
        - If ar_id does not exist, get the first one that has not been labeled.
        - If nothing left to label, return None
    '''
    ar_all  = fetch_all_ar(task_id, user_id)
    ar_done = set(fetch_all_annotations(task_id, user_id))

    try:
        idx = ar_all.index(ar_id)
    except:
        idx = 0
    end = idx
    idx = (idx + 1) % len(ar_all)

    while idx != end:
        _ar_id = ar_all[idx]
        if _ar_id not in ar_done:
            return _ar_id
        idx = (idx + 1) % len(ar_all)

    return None

def annotate_ar(task_id, user_id, ar_id, annotation):
    '''
    Annotate a annotation request
    '''
    if fetch_ar(task_id, user_id, ar_id) is not None:
        path = [_task_dir(task_id), DIR_ANNO, user_id, ar_id + '.json']
        mkf(*path)
        fname = os.path.join(*path)
        save_json(fname, annotation)
        return fname
    else:
        return None

def fetch_annotation(task_id, user_id, ar_id):
    '''
    Return the details of an annotation to a annotation request
    '''
    path = [_task_dir(task_id), DIR_ANNO, user_id, ar_id + '.json']
    fname = os.path.join(*path)
    return load_json(fname)

def fetch_all_annotations(task_id, user_id):
    '''
    Return a list of ar_id for this task that has been annotated by this user.
    '''
    _dir = os.path.join(_task_dir(task_id), DIR_ANNO, user_id)
    return _get_all_ar_ids_in_dir(_dir)

def _get_all_ar_ids_in_dir(_dir, sort_by_ctime=False):
    if os.path.isdir(_dir):
        dir_entries = list(os.scandir(_dir))
        if sort_by_ctime:
            dir_entries = sorted(dir_entries, key=lambda x: x.stat().st_ctime_ns, reverse=True)
        fnames = [x.name for x in dir_entries]
        ar_ids = [re.match('(.*).json$', f).groups()[0]
                for f in fnames]
        return ar_ids
    else:
        return []


if __name__ == '__main__':
    '''Some basic integration test'''

    task = Task('just testing ar')
    task.save()

    task_id = task.task_id
    user_id = 'eddie_test'

    # - New Annotation Requests.
    annotation_requests = [
        {
            'ar_id': get_ar_id('a', 1),
            'fname': 'a',
            'line_number': 1,

            'score': 0.9,
            'data': { 'text': 'blah', 'meta': {'foo': 'bar'}}
        },
        {
            'ar_id': get_ar_id('a', 2),
            'fname': 'a',
            'line_number': 2,

            'score': 0.5,
            'data': { 'text': 'blah', 'meta': {'foo': 'bar'}}
        },
        {
            'ar_id': get_ar_id('a', 3),
            'fname': 'a',
            'line_number': 3,

            'score': 0.1,
            'data': { 'text': 'blah', 'meta': {'foo': 'bar'}}
        },
    ]

    basedir = save_new_ar_for_user(task_id, user_id, annotation_requests)
    assert len(annotation_requests) == len(os.listdir(basedir))

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
    my_anno = {'label': 'HEALTHCARE'}
    annotate_ar(task_id, user_id, all_ars[0], my_anno)
    assert fetch_annotation(task_id, user_id, all_ars[0]) == my_anno
    assert len(fetch_all_annotations(task_id, user_id)) == 1

    # Annotate the same thing again updates the annotation
    updated_anno = {'label': 'FINTECH'}
    annotate_ar(task_id, user_id, all_ars[0], updated_anno)
    assert fetch_annotation(task_id, user_id, all_ars[0]) == updated_anno
    assert len(fetch_all_annotations(task_id, user_id)) == 1

    # Annotate something that doesn't exist
    # (this might happen when master is updating - we'll try to avoid it)
    annotate_ar(task_id, user_id, 'doesnotexist', {'label': 'HEALTHCARE'})
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
            'data': { 'text': 'blah', 'meta': {'foo': 'bar'}}
        }
    ]

    basedir = save_new_ar_for_user(task_id, user_id, new_annotation_requests)
    assert len(new_annotation_requests) == len(os.listdir(basedir))
    assert len(new_annotation_requests) == len(fetch_all_ar(task_id, user_id))

    # Check existing annotations still exist
    assert len(fetch_all_annotations(task_id, user_id)) == 1

    # Annotate something thing in the new batch
    my_anno = {'label': 'MACHINELEARNING'}
    all_ars = fetch_all_ar(task_id, user_id)
    annotate_ar(task_id, user_id, all_ars[0], my_anno)
    assert fetch_annotation(task_id, user_id, all_ars[0]) == my_anno
    assert len(fetch_all_annotations(task_id, user_id)) == 2

    # Nothing left to annotate
    assert get_next_ar(task_id, user_id, all_ars[0]) == None
