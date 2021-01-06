import logging
from typing import Optional, Dict, List

from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import Database, User, AnnotationRequest, LabelOwner, ClassificationAnnotation, Task

logger = logging.getLogger('okta_migration')
logger.setLevel(logging.INFO)


def _merge_users(session, merged_user: str, merge_to: str, create_users: bool):
    user: Optional[User] = (session.query(User)
                            .filter(User.username == merged_user)
                            .one_or_none())
    if user is None:
        logger.warning(f"Merge candidate {merged_user}(?) does not exist, skipping.")
        return
    target_user: Optional[User] = (
        session.query(User)
            .filter(User.username == merge_to)
            .one_or_none()
    )
    if not target_user:
        if not create_users:
            logger.warning(f"Skipping merges into {merge_to}(?) since it does not exist")
            return
        target_user = User(username=merge_to)
        session.add(target_user)

    (session.query(AnnotationRequest)
     .filter(AnnotationRequest.user_id == user.id)
     .update(dict(user_id=target_user.id)))

    (session.query(LabelOwner)
     .filter(LabelOwner.owner_id == user.id)
     .update(dict(owner_id=target_user.id)))

    (session.query(ClassificationAnnotation)
     .filter(ClassificationAnnotation.user_id == user.id)
     .update(dict(user_id=target_user.id)))

    logger.info(f"Merged user {merged_user}({user.id}) into {target_user.username}({target_user.id})")


def _replace_mapper(items_list):
    changed = False
    return_val = []
    for item in items_list:
        new_name = annotators_mapping.get(item, None)
        if new_name:
            changed = True
            return_val.append(new_name)
        else:
            return_val.extend(items_list)
    if changed:
        logger.info(f"Annotators list updated from {','.join(items_list)} to {','.join(return_val)}")
        return return_val
    logger.info('Annotators list not updated')
    del return_val
    return items_list


def _delete_merged_users(mergers, session):
    users_to_drop_list = []

    for old_users in mergers.values():
        users_to_drop_list.extend(old_users)

    affected_rows = session.query(User).filter(User.username.in_(users_to_drop_list)).delete(synchronize_session='fetch')
    logger.info(f"Deleted {affected_rows} Users")


def main(
        mergers: Dict[str, List[str]],
        annotators_mapping: Dict[str, str],
        create_users: bool = False
):
    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
    session = db.session

    logger.info("STEP 1: update FKs to Users table (merging duplicate users)")
    _perform_user_mergers(mergers, session, create_users)

    logger.info("STEP 2: Updating annotator names in task definitions to match the new okta usernames")
    _update_annotator_names(session)

    logger.info("STEP 3: Delete the merged usernames")
    _delete_merged_users(mergers, session)

    logger.info("STEP 3: Update the usernames in the Users table to match the new okta usernames")
    _update_usernames(annotators_mapping, session)

    session.commit()


def _update_usernames(annotators_mapping, session):
    # Update remaining usernames:
    users_to_update_list = []
    for user in session.query(User).all():
        un = str(user.username)
        if un not in annotators_mapping:
            logger.warning(f"Not updating user {un} in Users table, it does not exist")
            continue

        user.username = annotators_mapping[un]
        users_to_update_list.append(user)

    affected_rows = session.add_all(users_to_update_list)
    logger.info(f"Updated {affected_rows} Users")


def _update_annotator_names(session):
    # Update annotator names
    updated_tasks = []
    for task in session.query(Task).all():
        old_annotators = task.default_params['annotators']
        new_annotators = _replace_mapper(old_annotators)
        if new_annotators is not old_annotators:
            dp = task.default_params.copy()
            dp['annotators'] = new_annotators
            task.default_params = dp
            updated_tasks.append(task)
    logger.info(f"Updated {len(updated_tasks)} tasks")
    session.add_all(updated_tasks)


def _perform_user_mergers(mergers, session, create_users):
    # Merge users
    for merge_to in mergers:
        for merged_user in mergers[merge_to]:
            _merge_users(session, merged_user, merge_to, create_users)


if __name__ == '__main__':
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Migrate the data according to specified mappings")
    parser.add_argument("mapping", type=argparse.FileType('r'))
    parser.add_argument("--create-users", default=False, action="store_true", help="Create non-existing users")
    args = parser.parse_args()

    mapping_file = json.loads(args.mapping.read())

    # From users table.
    # "A": ["B", "C"] means that all FKs to B and C should be updated to point to "A".
    # So B and C still exist on DB but no foreign keys point to them anymore.
    mergers = mapping_file['mergers']

    # Result of:
    # select distinct json_array_elements_text(default_params->'annotators') as name from task order by name;
    #
    # This is used to update the annotator usernames in task definitions also the users in Users table
    annotators_mapping = mapping_file['annotators_mapping']

    main(mergers, annotators_mapping, args.create_users)
