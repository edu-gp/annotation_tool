#!/usr/bin/env python3

import logging

from alchemy.db.model import Database, EntityTypeEnum, Model

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    db = Database.bootstrap()
    result = db.session.query(Model).all()

    for model in result:
        if model.entity_type is None:
            model.entity_type = EntityTypeEnum.COMPANY
            db.session.add(model)
    db.session.commit()
    db.session.close()
