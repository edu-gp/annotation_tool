from alchemy.db.model import (
    ClassificationTrainingData,
    Model,
    Task,
    TextClassificationModel,
)


def test_flow(dbsession):
    # User creates a task with the label they're interested in
    task = Task(name="My Task", default_params={"labels": ["Smart"]})

    dbsession.add(task)
    dbsession.commit()
    dbsession.refresh(task)

    # Pretend there are some data for this labels, we go ahead and take a
    # snapshot and convert it to training data.
    training_data = ClassificationTrainingData()
    training_data.label = "Smart"

    dbsession.add(training_data)
    dbsession.commit()
    dbsession.refresh(training_data)

    assert training_data.path() is not None
    assert training_data.label == "Smart"

    # A model training job is kicked off. A background job runs it and in the
    # case that it succeeds, we save the model and any post-training inferences
    # that were completed.

    model = TextClassificationModel(label="Smart")

    dbsession.add(model)

    dbsession.commit()
    dbsession.refresh(model)

    text_classification_models = (
        dbsession.query(TextClassificationModel)
        .filter(TextClassificationModel.label == "Smart")
        .all()
    )

    assert len(text_classification_models) == 1

    models = dbsession.query(Model).filter(Model.label == "Smart").all()

    assert len(models) == 1
