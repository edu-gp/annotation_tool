class DeployedInferenceMetadata:
    def __init__(self, model_uuid, model_version, threshold, filename):
        self.model_uuid = model_uuid
        self.model_version = model_version
        self.threshold = threshold
        self.filename = filename

    def to_dict(self):
        return {
            'model_uuid': self.model_uuid,
            'model_version': self.model_version,
            'threshold': self.threshold,
            'filename': self.filename
        }

    @classmethod
    def from_dict(cls, obj):
        return cls(
            model_uuid=obj.get('model_uuid'),
            model_version=obj.get('model_version'),
            threshold=obj.get('threshold'),
            filename=obj.get('filename')
        )


def download_file(filename):
    # TODO
    pass


def get_selected_deployment_configs(dbsession):
    # TODO
    pass


def already_has_inference(model, filename):
    """Check if inference has already ran on this file with the given config"""
    # TODO
    pass


def build_results_for_production(metadata: DeployedInferenceMetadata):
    """Idempotent call.
    Creates files in the production GCS bucket if the files are there.
    If files are created/updated, it also fires a pubsub message.
    """
    # TODO
    pass
