from db.model import ModelDeploymentConfig, TextClassificationModel


def test_get_selected_for_deployment(dbsession):
    # TODO mock out a fully trained model
    model = TextClassificationModel(uuid='123', version=1)
    dbsession.add(model)
    dbsession.commit()

    config_a = ModelDeploymentConfig(
        model_id=model.id,
        is_approved=False,
        is_selected_for_deployment=False,
        threshold=0.7
    )
    config_b = ModelDeploymentConfig(
        model_id=model.id,
        is_approved=True,
        is_selected_for_deployment=False,
        threshold=0.8
    )
    config_c = ModelDeploymentConfig(
        model_id=model.id,
        is_approved=True,
        is_selected_for_deployment=True,
        threshold=0.9
    )

    dbsession.add_all([config_a, config_b, config_c])
    dbsession.commit()

    res = ModelDeploymentConfig.get_selected_for_deployment(dbsession)
    assert res == [config_c]

    config_b.is_selected_for_deployment = True
    dbsession.add(config_b)
    dbsession.commit()
    res = ModelDeploymentConfig.get_selected_for_deployment(dbsession)
    assert res == [config_b, config_c]  # Note: Natural ordering by PK.
