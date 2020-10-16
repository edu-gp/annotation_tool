import time

import pandas as pd
from tqdm import tqdm

from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import ClassificationAnnotation, Database

db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)

if __name__ == "__main__":
    query = db.session.query(ClassificationAnnotation)

    count = query.count()
    print(f"Exporting {count} Annotations")

    bs = 50

    res = []
    for el in tqdm(query.yield_per(bs), total=count):
        res.append((el.id, el.entity_type, el.entity, el.user_id, el.label, el.value))

    res = pd.DataFrame(
        res, columns=["id", "entity_type", "entity", "user_id", "label", "value"]
    )

    now = int(time.time())
    res.to_csv(f"/tmp/annotations_{now}.csv", index=False)
