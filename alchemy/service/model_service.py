"""This class handles model training, management and inference."""


class ModelService:
    def __init__(self, data_manager, model_manager):
        self.data_manager = data_manager
        self.model_manager = model_manager

    def submit_remote_training_job(self):
        pass

    def submit_remote_inference_job(self):
        pass

    def download_training_data(self):
        # self.data_manager.download_training_data()
        pass

    def download_predictions(self):
        # self.data_manager.download_predictions()
        pass

    def retrieve_model_assets(self):
        pass

    def retrieve_model_stats(self):
        pass
