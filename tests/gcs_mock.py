import pathlib
import tempfile


class Bucket:
    def __init__(self, name):
        self.name = name

    def copy_blob(self, blob, destination_bucket, new_name):
        src = Blob.tmp_dir / self.name / blob.name
        dst = Blob.tmp_dir / destination_bucket.name / new_name
        import shutil
        shutil.copyfile(str(src), str(dst))


class Blob:
    tmp_dir = pathlib.Path(tempfile.gettempdir()) / 'test'

    def __init__(self, name, bucket, **kwargs):
        self._name = name
        self._bucket = bucket
        self.metadata = dict()

    def _get_file(self):
        file = Blob.tmp_dir / self._bucket.name / self._name
        return file

    def exists(self):
        return self._get_file().exists()

    def upload_from_filename(self, filename):
        with open(filename, 'rb') as f:
            self.upload_from_file(f)

    def upload_from_file(self, file):
        self._get_file().parent.mkdir(parents=True, exist_ok=True)
        self._get_file().write_bytes(file.read())

    def upload_from_string(self, string, encoding='utf-8'):
        self._get_file().parent.mkdir(parents=True, exist_ok=True)
        self._get_file().write_text(string, encoding=encoding)

    def download_as_text(self, encoding='utf-8'):
        return self._get_file().read_text(encoding=encoding)

    def download_to_file(self, file):
        file.write(self._get_file().read_bytes())
        file.flush()

    def download_to_filename(self, file_name):
        with open(file_name, 'wb') as file:
            return self.download_to_file(file)

    @property
    def name(self):
        return self._name

    @property
    def bucket(self):
        return Bucket(self._bucket.name)

    @classmethod
    def from_string(cls, string):
        # string= gs://{bucket_name}/{name}
        import re
        matches = re.match(re.compile("gs://([^/]+)/(.+)"), string)
        bucket_name = matches.group(1)
        name = matches.group(2)
        return cls(name, Bucket(bucket_name))


class Client:
    def list_blobs(self, bucket, prefix):
        folder = Blob.tmp_dir / bucket.name / prefix
        if not folder.is_dir():
            folder = folder.parent
            prefix = prefix[prefix.rindex('/')+1:]
        else:
            prefix = ''
        return [
            Blob(f.name, bucket)
            for f in folder.iterdir()
            if f.name.startswith(prefix)
        ]
