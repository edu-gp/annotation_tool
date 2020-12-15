import logging
from typing import Optional, Dict, List

from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import Database, User, AnnotationRequest, LabelOwner, ClassificationAnnotation, Task

logger = logging.getLogger('okta_migration')


def _merge_users(session, merged_user: str, target_user: User):
    user: Optional[User] = (session.query(User)
                            .filter(User.username == merged_user)
                            .one_or_none())
    if user is None:
        logger.warning(f"Merge candidate {merged_user}(?) does not exist, skipping.")
        return
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
            return_val.append(items_list)

    if changed:
        logger.info(f"Annotators list updated from {','.join(items_list)} to {','.join(return_val)}")
        return return_val
    logger.info('Annotators list not updated')
    del return_val
    return items_list


def main(mergers: Dict[str, List[str]], annotators_mapping: Dict[str, str], dry_run: bool = False):
    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
    session = db.session

    logger.info("STEP 1: update FKs to Users table (merging duplicate users)")
    _perform_user_mergers(mergers, session)

    logger.info("STEP 2: Updating annotator names in task definitions to match the new okta usernames")
    _update_annotator_names(session)

    logger.info("STEP 3: Update the usernames in the Users table to match the new okta usernames")
    _update_usernames(annotators_mapping, session)

    if dry_run:
        logger.info('Rolling back the changes (dry run)')
        session.rollback()
    else:
        logger.info('Committing changes to the DB')
        session.commit()


def _update_usernames(annotators_mapping, session):
    # Update remaining usernames:
    users_to_update_list = []
    for user in session.query(User).all():
        un = str(user.username)
        if un not in annotators_mapping:
            logger.warning(f"Not updating user {un} in Users table")
            continue

        user.username = annotators_mapping[un]
        users_to_update_list.append(user)
    session.add_all(users_to_update_list)


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


def _perform_user_mergers(mergers, session):
    # Merge users
    for merge_to in mergers:
        merge_to_user: Optional[User] = (
            session.query(User)
                .filter(User.username == merge_to)
                .one_or_none()
        )

        if merge_to_user is None:
            logger.warning(f"Skipping merges into {merge_to}(?) since it does not exist")
            continue

        for merged_user in mergers[merge_to]:
            _merge_users(session, merged_user, merge_to_user)


if __name__ == '__main__':
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Migrate the data according to specified mappings")
    parser.add_argument("mapping", type=argparse.FileType('r'))
    parser.add_argument("--dry", default=False, action="store_true", help="Dry run")
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

    main(mergers, annotators_mapping, args.dry)
