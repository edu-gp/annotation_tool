"""
Models used to be stored on Tasks.
This script migrates all the models from binary-classification tasks
to individual labels (by storing the 'label' properly in Model).
"""
from db.model import Database, Task
from db.config import DevelopmentConfig

db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)

if __name__ == "__main__":
    for task in db.session.query(Task).all():
        print(task.name)

        labels = task.get_labels()
        if len(labels) == 1:
            label = labels[0]
            print(f'task="{task.name}" id={task.id} label="{label}"')

            for model in task.models:
                print(f'Associate label="{label}" with model id={model.id}')
                model.label = label
                db.session.add(model)
            db.session.commit()
        else:
            print(f'** Skip task name="{task.name}" id={task.id}, '
                  f'it has {len(labels)} labels.')
        print('-'*80)
