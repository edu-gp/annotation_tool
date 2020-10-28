#!/usr/bin/env python3

import datetime
import logging

import pytz

from alchemy.db.model import Database, EntityTypeEnum, Task

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    db = Database.bootstrap()
    result = db.session.query(Task).all()

    cutoff = datetime.datetime(2020, 7, 10).replace(tzinfo=pytz.UTC)

    for task in result:
        if task.created_at < cutoff:
            task.set_entity_type(EntityTypeEnum.COMPANY)
            db.session.add(task)
    db.session.commit()
    db.session.close()
