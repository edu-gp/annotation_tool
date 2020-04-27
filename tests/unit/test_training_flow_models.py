from tests.sqlalchemy_conftest import *
from db.model import (
    EntityType, Label,
    Task,
    ClassificationTrainingData,
    TextClassificationModel,
    FileInference,
)


def test_flow(dbsession):
    # Given an entity and label
    entity_type = EntityType(name='Person')
    label = Label(name='Smart')
    entity_type.labels.append(label)

    dbsession.add(entity_type)
    dbsession.commit()
    dbsession.refresh(entity_type)

    # User creates a task with the label they're interested in
    task = Task(
        name="My Task",
        default_params={
            'labels': [label.name]
        }
    )

    dbsession.add(task)
    dbsession.commit()
    dbsession.refresh(task)

    # Pretend there are some data for this labels, we go ahead and take a
    # snapshot and convert it to training data.
    training_data = ClassificationTrainingData()
    training_data.label = label

    dbsession.add(training_data)
    dbsession.commit()
    dbsession.refresh(training_data)

    assert training_data.path() is not None
    assert training_data.label == label

    # A model training job is kicked off. A background job runs it and in the
    # case that it succeeds, we save the model and any post-training inferences
    # that were completed.

    model = TextClassificationModel(
        task=task,
        data={
            'data_fname': training_data.path(),
        }
    )

    dbsession.add(model)

    inference = FileInference(
        model=model,
        input_filename='/my_raw_data.jsonl'
    )

    dbsession.add(inference)

    dbsession.commit()
    dbsession.refresh(model)
    dbsession.refresh(inference)

    assert inference.path() is not None

    # After everything, check if the relationships are right.
    dbsession.refresh(task)
    assert len(task.models) == 1
    assert len(task.text_classification_models.all()) == 1

    dbsession.refresh(inference)
    assert inference.model == model
