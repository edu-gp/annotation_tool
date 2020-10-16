#!/usr/bin/env python3

import logging

from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import Database, Task, EntityTypeEnum

import datetime
import pytz

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
    result = db.session.query(Task).all()
    
    cutoff = datetime.datetime(2020, 7, 10).replace(tzinfo=pytz.UTC)

    for task in result:
        if task.created_at < cutoff:
            task.set_entity_type(EntityTypeEnum.COMPANY)
            db.session.add(task)
    db.session.commit()
    db.session.close()
