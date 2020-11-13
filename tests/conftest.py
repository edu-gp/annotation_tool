from .sqlalchemy_conftest import *  # noqa


@pytest.fixture(scope="session", autouse=True)
def set_up_gcs_mock_tempdir(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp('gcloud')

    import google.cloud.storage

    from .gcs_mock import Client as FakeClient
    google.cloud.storage.__Client = google.cloud.storage.Client
    google.cloud.storage.Client = FakeClient
    google.cloud.storage.client.Client = FakeClient

    from .gcs_mock import Blob as FakeBlob
    google.cloud.storage.__Blob = google.cloud.storage.Blob
    google.cloud.storage.Blob = FakeBlob
    google.cloud.storage.blob.Blob = FakeBlob

    FakeBlob.tmp_dir = tmp_path
